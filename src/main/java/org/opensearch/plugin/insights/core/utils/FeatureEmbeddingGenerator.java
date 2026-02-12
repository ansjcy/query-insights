/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.client.Client;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.env.Environment;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;

/**
 * Utility class to generate feature embeddings for Search Query Records.
 * This class is responsible for generating feature vectors that can be used for model training
 * to predict query performance metrics such as latency, CPU usage, and memory consumption.
 */
public class FeatureEmbeddingGenerator {
    private static final Logger log = LogManager.getLogger(FeatureEmbeddingGenerator.class);

    // Feature embedding export settings
    private final boolean featureEmbeddingEnabled;
    private final String outputDirectory;
    private final String filePrefix;
    private final TimeValue exportInterval;
    private final List<String> featuresToInclude;
    private final ClusterService clusterService;
    private final Client client;
    private final QueryShapeGenerator queryShapeGenerator;

    // Feature header constants
    private static final String TIMESTAMP_MS = "timestamp_ms";
    private static final String QUERY_HASHCODE = "query_hashcode";
    private static final String INDICES_COUNT = "indices_count";
    private static final String TOTAL_SHARDS = "total_shards";
    private static final String SEARCH_TYPE = "search_type";
    private static final String HAS_AGGREGATIONS = "has_aggregations";
    private static final String AGGREGATIONS_COUNT = "aggregations_count";
    private static final String HAS_SORT = "has_sort";
    private static final String SORT_COUNT = "sort_count";
    private static final String HAS_SOURCE_FILTERING = "has_source_filtering";
    private static final String SOURCE_INCLUDES_COUNT = "source_includes_count";
    private static final String SOURCE_EXCLUDES_COUNT = "source_excludes_count";
    private static final String HAS_QUERY = "has_query";
    private static final String QUERY_TYPE = "query_type";
    private static final String QUERY_DEPTH = "query_depth";
    private static final String HAS_SIZE = "has_size";
    private static final String SIZE_VALUE = "size_value";
    private static final String HAS_FROM = "has_from";
    private static final String FROM_VALUE = "from_value";
    private static final String ACTIVE_NODES_COUNT = "active_nodes_count";
    private static final String CLUSTER_STATUS = "cluster_status";
    private static final String INDICES_STATUS = "indices_status";
    private static final String AVG_CPU_UTILIZATION = "avg_cpu_utilization";
    private static final String AVG_JVM_HEAP_USED_PERCENT = "avg_jvm_heap_used_percent";
    private static final String AVG_DISK_USED_PERCENT = "avg_disk_used_percent";
    private static final String AVG_DOCS_COUNT = "avg_docs_count";
    private static final String MAX_DOCS_COUNT = "max_docs_count";
    private static final String AVG_INDEX_SIZE_BYTES = "avg_index_size_bytes";
    private static final String TIME_OF_DAY_HOUR = "time_of_day_hour";
    private static final String DAY_OF_WEEK = "day_of_week";
    private static final String LATENCY_MS = "latency_ms";
    private static final String CPU_NANOS = "cpu_nanos";
    private static final String MEMORY_BYTES = "memory_bytes";

    // Feature vectors CSV header
    private final List<String> headerColumns = Arrays.asList(
        TIMESTAMP_MS,
        QUERY_HASHCODE,
        INDICES_COUNT,
        TOTAL_SHARDS,
        SEARCH_TYPE,
        HAS_AGGREGATIONS,
        AGGREGATIONS_COUNT,
        HAS_SORT,
        SORT_COUNT,
        HAS_SOURCE_FILTERING,
        SOURCE_INCLUDES_COUNT,
        SOURCE_EXCLUDES_COUNT,
        HAS_QUERY,
        QUERY_TYPE,
        QUERY_DEPTH,
        HAS_SIZE,
        SIZE_VALUE,
        HAS_FROM,
        FROM_VALUE,
        ACTIVE_NODES_COUNT,
        CLUSTER_STATUS,
        INDICES_STATUS,
        AVG_CPU_UTILIZATION,
        AVG_JVM_HEAP_USED_PERCENT,
        AVG_DISK_USED_PERCENT,
        AVG_DOCS_COUNT,
        MAX_DOCS_COUNT,
        AVG_INDEX_SIZE_BYTES,
        TIME_OF_DAY_HOUR,
        DAY_OF_WEEK,
        LATENCY_MS,
        CPU_NANOS,
        MEMORY_BYTES
    );

    // Last export time to track when to write to file
    private long lastExportTime = 0;
    // In-memory buffer for feature vectors before writing to file
    private final List<Map<String, Object>> featureVectorsBuffer = new ArrayList<>();

    /**
     * Constructor for FeatureEmbeddingGenerator
     *
     * @param settings The node settings
     * @param clusterService The node's cluster service
     * @param client The client for OpenSearch operations
     * @param queryShapeGenerator The generator for query shapes
     */
    public FeatureEmbeddingGenerator(Settings settings, ClusterService clusterService, Client client, QueryShapeGenerator queryShapeGenerator, Environment env) {
        this.clusterService = clusterService;
        this.client = client;
        this.queryShapeGenerator = queryShapeGenerator;

        // Load configuration from settings
        this.featureEmbeddingEnabled = settings.getAsBoolean("search.insights.feature_embedding.enabled", true);
        this.outputDirectory = settings.get("search.insights.feature_embedding.output_directory", env.tmpDir().toString());
        this.filePrefix = settings.get("search.insights.feature_embedding.file_prefix", "query_features");
        this.exportInterval = settings.getAsTime(
            "search.insights.feature_embedding.export_interval",
            TimeValue.timeValueSeconds(10)
        );

        // Parse the features to include, default to all
        String featuresToIncludeString = settings.get("search.insights.feature_embedding.features_to_include", "all");
        if ("all".equalsIgnoreCase(featuresToIncludeString)) {
            this.featuresToInclude = headerColumns;
        } else {
            this.featuresToInclude = Arrays.asList(featuresToIncludeString.split(","));
        }

        // Ensure output directory exists
        try {
            Path dirPath = Paths.get(outputDirectory);
            Files.createDirectories(dirPath);
            log.info("Feature embedding output directory: {}", outputDirectory);
        } catch (IOException e) {
            log.error("Failed to create output directory for feature embeddings: {}", e.getMessage());
        }

        // Initialize with headers if enabled
        if (featureEmbeddingEnabled) {
            log.info("Feature embedding generation is enabled. Output directory: {}", outputDirectory);
            initializeOutputFile();
        } else {
            log.info("Feature embedding generation is disabled.");
        }
    }

    /**
     * Initializes the output CSV file with headers if it doesn't exist
     */
    private void initializeOutputFile() {
        String filename = generateFilename();
        File outputFile = new File(outputDirectory, filename);

        // Only create a new file with headers if it doesn't exist
        if (!outputFile.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                // Write header
                String headerLine = headerColumns.stream()
                    .collect(Collectors.joining(","));
                writer.write(headerLine);
                writer.newLine();
                log.info("Created feature embedding file with headers: {}", outputFile.getAbsolutePath());
            } catch (IOException e) {
                log.error("Failed to initialize feature embedding file: {}", e.getMessage());
            }
        }
    }

    /**
     * Generates a filename for the CSV output based on the current date
     *
     * @return The generated filename
     */
    private String generateFilename() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return String.format("%s_%s.csv", filePrefix, now.format(formatter));
    }

    /**
     * Processes a search query and generates feature embeddings
     *
     * @param context The search phase context
     * @param searchRequestContext The search request context
     * @return A map of feature name to feature value
     */
    public Map<String, Object> generateFeatureVector(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        if (!featureEmbeddingEnabled) {
            return Collections.emptyMap();
        }

        try {
            SearchRequest request = context.getRequest();
            Map<String, Object> featureVector = new HashMap<>();

            // Extract basic query attributes
            featureVector.put(TIMESTAMP_MS, request.getOrCreateAbsoluteStartMillis());

            // Generate query hash code for identifying similar queries
            if (request.source() != null) {
                String queryShape = queryShapeGenerator.buildShape(
                    request.source(),
                    true,  // Include field names
                    true,  // Include field types
                    searchRequestContext.getSuccessfulSearchShardIndices()
                );
                String hashcode = queryShapeGenerator.getShapeHashCodeAsString(queryShape);
                featureVector.put(QUERY_HASHCODE, hashcode);
            } else {
                featureVector.put(QUERY_HASHCODE, "null");
            }

            // Extract indices information
            String[] indices = request.indices();
            featureVector.put(INDICES_COUNT, indices != null ? indices.length : 0);
            featureVector.put(TOTAL_SHARDS, context.getNumShards());

            // Extract search type
            featureVector.put(SEARCH_TYPE, request.searchType().toString().toLowerCase(Locale.ROOT));

            // Extract query source details
            SearchSourceBuilder source = request.source();
            if (source != null) {
                // Aggregations
                List<AggregationBuilder> aggregations = source.aggregations() != null ?
                    new ArrayList<>(source.aggregations().getAggregatorFactories()) :
                    Collections.emptyList();
                featureVector.put(HAS_AGGREGATIONS, !aggregations.isEmpty() ? 1 : 0);
                featureVector.put(AGGREGATIONS_COUNT, aggregations.size());

                // Sort
                List<SortBuilder<?>> sorts = source.sorts() != null ? source.sorts() : Collections.emptyList();
                featureVector.put(HAS_SORT, !sorts.isEmpty() ? 1 : 0);
                featureVector.put(SORT_COUNT, sorts.size());

                // Source filtering
                boolean hasSourceFiltering = source.fetchSource() != null &&
                    (source.fetchSource().includes() != null || source.fetchSource().excludes() != null);
                featureVector.put(HAS_SOURCE_FILTERING, hasSourceFiltering ? 1 : 0);

                if (hasSourceFiltering && source.fetchSource().includes() != null) {
                    featureVector.put(SOURCE_INCLUDES_COUNT, source.fetchSource().includes().length);
                } else {
                    featureVector.put(SOURCE_INCLUDES_COUNT, 0);
                }

                if (hasSourceFiltering && source.fetchSource().excludes() != null) {
                    featureVector.put(SOURCE_EXCLUDES_COUNT, source.fetchSource().excludes().length);
                } else {
                    featureVector.put(SOURCE_EXCLUDES_COUNT, 0);
                }

                // Query
                featureVector.put(HAS_QUERY, source.query() != null ? 1 : 0);
                featureVector.put(QUERY_TYPE, source.query() != null ? source.query().getName() : "none");
                featureVector.put(QUERY_DEPTH, source.query() != null ? calculateQueryDepth(source.query().toString()) : 0);

                // Size and From
                featureVector.put(HAS_SIZE, source.size() != -1 ? 1 : 0);
                featureVector.put(SIZE_VALUE, source.size() != -1 ? source.size() : 10);  // Default OpenSearch size is 10
                featureVector.put(HAS_FROM, source.from() != -1 ? 1 : 0);
                featureVector.put(FROM_VALUE, source.from() != -1 ? source.from() : 0);   // Default OpenSearch from is 0
            } else {
                // Default values for null source
                featureVector.put(HAS_AGGREGATIONS, 0);
                featureVector.put(AGGREGATIONS_COUNT, 0);
                featureVector.put(HAS_SORT, 0);
                featureVector.put(SORT_COUNT, 0);
                featureVector.put(HAS_SOURCE_FILTERING, 0);
                featureVector.put(SOURCE_INCLUDES_COUNT, 0);
                featureVector.put(SOURCE_EXCLUDES_COUNT, 0);
                featureVector.put(HAS_QUERY, 0);
                featureVector.put(QUERY_TYPE, "none");
                featureVector.put(QUERY_DEPTH, 0);
                featureVector.put(HAS_SIZE, 0);
                featureVector.put(SIZE_VALUE, 10);
                featureVector.put(HAS_FROM, 0);
                featureVector.put(FROM_VALUE, 0);
            }

            // Fetch cluster and index stats asynchronously to avoid blocking
            try {
                // Cluster health information
                ClusterHealthResponse clusterHealthResponse = client.admin()
                    .cluster()
                    .prepareHealth()
                    .get();

                featureVector.put(ACTIVE_NODES_COUNT, clusterHealthResponse.getNumberOfNodes());
                featureVector.put(CLUSTER_STATUS, clusterHealthResponse.getStatus().toString());

                if (indices != null && indices.length > 0) {
                    // Get health status for the indices used in the query
                    Map<String, ClusterIndexHealth> indexHealthMap = clusterHealthResponse.getIndices();
                    double avgStatus = 0.0;
                    int count = 0;

                    for (String index : indices) {
                        ClusterIndexHealth indexHealth = indexHealthMap.get(index);
                        if (indexHealth != null) {
                            // Convert status to numeric: RED=0, YELLOW=1, GREEN=2
                            int statusValue = indexHealth.getStatus().value();
                            avgStatus += statusValue;
                            count++;
                        }
                    }

                    if (count > 0) {
                        avgStatus /= count;
                    }

                    featureVector.put(INDICES_STATUS, avgStatus);
                } else {
                    featureVector.put(INDICES_STATUS, -1);
                }

                // Node stats information
                NodesStatsResponse nodesStats = client.admin()
                    .cluster()
                    .prepareNodesStats()
                    .all()
                    .get();

                double avgCpuUtilization = 0.0;
                double avgJvmHeapUsedPercent = 0.0;
                double avgDiskUsedPercent = 0.0;
                int nodeCount = 0;

                for (NodeStats nodeStats : nodesStats.getNodes()) {
                    nodeCount++;

                    // CPU utilization
                    OsStats osStats = nodeStats.getOs();
                    if (osStats != null && osStats.getCpu() != null) {
                        avgCpuUtilization += osStats.getCpu().getPercent();
                    }

                    // JVM heap usage
                    JvmStats jvmStats = nodeStats.getJvm();
                    if (jvmStats != null) {
                        avgJvmHeapUsedPercent += jvmStats.getMem().getHeapUsedPercent();
                    }

                    // Disk usage
                    FsInfo fsInfo = nodeStats.getFs();
                    if (fsInfo != null && fsInfo.getTotal() != null) {
                        long total = fsInfo.getTotal().getTotal().getBytes();
                        long free = fsInfo.getTotal().getAvailable().getBytes();
                        if (total > 0) {
                            double usedPercent = 100.0 * (total - free) / total;
                            avgDiskUsedPercent += usedPercent;
                        }
                    }
                }

                if (nodeCount > 0) {
                    featureVector.put(AVG_CPU_UTILIZATION, avgCpuUtilization / nodeCount);
                    featureVector.put(AVG_JVM_HEAP_USED_PERCENT, avgJvmHeapUsedPercent / nodeCount);
                    featureVector.put(AVG_DISK_USED_PERCENT, avgDiskUsedPercent / nodeCount);
                } else {
                    featureVector.put(AVG_CPU_UTILIZATION, -1);
                    featureVector.put(AVG_JVM_HEAP_USED_PERCENT, -1);
                    featureVector.put(AVG_DISK_USED_PERCENT, -1);
                }

                // Index stats
                if (indices != null && indices.length > 0) {
                    IndicesStatsResponse indicesStats = client.admin()
                        .indices()
                        .prepareStats(indices)
                        .get();

                    long totalDocs = 0;
                    long maxDocs = 0;
                    long totalSizeBytes = 0;
                    int indexCount = 0;

                    for (String index : indices) {
                        if (indicesStats.getIndex(index) != null) {
                            indexCount++;
                            long docsCount = indicesStats.getIndex(index).getPrimaries().getDocs().getCount();
                            totalDocs += docsCount;
                            maxDocs = Math.max(maxDocs, docsCount);
                            totalSizeBytes += indicesStats.getIndex(index).getPrimaries().getStore().getSizeInBytes();
                        }
                    }

                    if (indexCount > 0) {
                        featureVector.put(AVG_DOCS_COUNT, (double) totalDocs / indexCount);
                        featureVector.put(MAX_DOCS_COUNT, maxDocs);
                        featureVector.put(AVG_INDEX_SIZE_BYTES, (double) totalSizeBytes / indexCount);
                    } else {
                        featureVector.put(AVG_DOCS_COUNT, -1);
                        featureVector.put(MAX_DOCS_COUNT, -1);
                        featureVector.put(AVG_INDEX_SIZE_BYTES, -1);
                    }
                } else {
                    featureVector.put(AVG_DOCS_COUNT, -1);
                    featureVector.put(MAX_DOCS_COUNT, -1);
                    featureVector.put(AVG_INDEX_SIZE_BYTES, -1);
                }
            } catch (Exception e) {
                log.warn("Failed to gather cluster/index stats for feature embedding: {}", e.getMessage());
                featureVector.put(ACTIVE_NODES_COUNT, -1);
                featureVector.put(CLUSTER_STATUS, "unknown");
                featureVector.put(INDICES_STATUS, -1);
                featureVector.put(AVG_CPU_UTILIZATION, -1);
                featureVector.put(AVG_JVM_HEAP_USED_PERCENT, -1);
                featureVector.put(AVG_DISK_USED_PERCENT, -1);
                featureVector.put(AVG_DOCS_COUNT, -1);
                featureVector.put(MAX_DOCS_COUNT, -1);
                featureVector.put(AVG_INDEX_SIZE_BYTES, -1);
            }

            // Add time-based features
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(request.getOrCreateAbsoluteStartMillis()),
                ZoneId.systemDefault()
            );
            featureVector.put(TIME_OF_DAY_HOUR, dateTime.getHour());
            featureVector.put(DAY_OF_WEEK, dateTime.getDayOfWeek().getValue());

            // Add performance metrics (will be set later in the listener)
            featureVector.put(LATENCY_MS, 0L);
            featureVector.put(CPU_NANOS, 0L);
            featureVector.put(MEMORY_BYTES, 0L);
            log.info("Generated Feature Vector!!");
            return featureVector;
        } catch (Exception e) {
            log.error("Error generating feature vector: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Updates the performance metrics in the feature vector
     *
     * @param featureVector The feature vector to update
     * @param record The search query record containing performance metrics
     */
    public void updatePerformanceMetrics(Map<String, Object> featureVector, SearchQueryRecord record) {
        if (featureVector == null || record == null) {
            return;
        }

        // Update the performance metrics in the feature vector
        featureVector.put(LATENCY_MS, record.getMeasurement(MetricType.LATENCY).longValue());
        featureVector.put(CPU_NANOS, record.getMeasurement(MetricType.CPU).longValue());
        featureVector.put(MEMORY_BYTES, record.getMeasurement(MetricType.MEMORY).longValue());
    }

    /**
     * Exports the feature vector to the CSV file
     *
     * @param featureVector The feature vector to export
     */
    public synchronized void exportFeatureVector(Map<String, Object> featureVector) {
        if (!featureEmbeddingEnabled || featureVector == null || featureVector.isEmpty()) {
            return;
        }

        featureVectorsBuffer.add(featureVector);

        // Check if it's time to flush to file
        long currentTime = System.currentTimeMillis();
        if (lastExportTime == 0) {
            lastExportTime = currentTime;
        }

        if (currentTime - lastExportTime >= exportInterval.getMillis() || featureVectorsBuffer.size() >= 1000) {
            flushToFile();
            lastExportTime = currentTime;
        }
    }

    /**
     * Calculates the depth of a query by counting the number of nested levels
     *
     * @param queryString The query string
     * @return The query depth
     */
    private int calculateQueryDepth(String queryString) {
        if (queryString == null || queryString.isEmpty()) {
            return 0;
        }

        int maxDepth = 0;
        int currentDepth = 0;

        for (char c : queryString.toCharArray()) {
            if (c == '{' || c == '[') {
                currentDepth++;
                maxDepth = Math.max(maxDepth, currentDepth);
            } else if (c == '}' || c == ']') {
                currentDepth--;
            }
        }

        return maxDepth;
    }

    /**
     * Writes the buffered feature vectors to the CSV file
     */
    private synchronized void flushToFile() {
        if (featureVectorsBuffer.isEmpty()) {
            return;
        }

        String filename = generateFilename();
        File outputFile = new File(outputDirectory, filename);
        boolean fileExists = outputFile.exists();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, fileExists))) {
            // Write header if file doesn't exist
            if (!fileExists) {
                String headerLine = headerColumns.stream()
                    .collect(Collectors.joining(","));
                writer.write(headerLine);
                writer.newLine();
            }

            // Write data rows
            for (Map<String, Object> featureVector : featureVectorsBuffer) {
                List<String> rowValues = new ArrayList<>();

                for (String column : headerColumns) {
                    Object value = featureVector.getOrDefault(column, "");

                    // Format value appropriately for CSV
                    String formattedValue;
                    if (value == null) {
                        formattedValue = "";
                    } else if (value instanceof String) {
                        // Escape quotes and enclose in quotes if it contains comma
                        String strValue = (String) value;
                        if (strValue.contains(",") || strValue.contains("\"") || strValue.contains("\n")) {
                            formattedValue = "\"" + strValue.replace("\"", "\"\"") + "\"";
                        } else {
                            formattedValue = strValue;
                        }
                    } else {
                        formattedValue = String.valueOf(value);
                    }

                    rowValues.add(formattedValue);
                }

                writer.write(String.join(",", rowValues));
                writer.newLine();
            }

            log.debug("Wrote {} feature vectors to CSV file: {}", featureVectorsBuffer.size(), outputFile.getAbsolutePath());
            featureVectorsBuffer.clear();
        } catch (IOException e) {
            log.error("Failed to write feature vectors to CSV: {}", e.getMessage());
        }
    }

    /**
     * Forces a flush of any buffered feature vectors to the file
     */
    public void forceFlush() {
        flushToFile();
    }

    /**
     * Checks if feature embedding generation is enabled
     *
     * @return True if enabled, false otherwise
     */
    public boolean isEnabled() {
        return featureEmbeddingEnabled;
    }
}
