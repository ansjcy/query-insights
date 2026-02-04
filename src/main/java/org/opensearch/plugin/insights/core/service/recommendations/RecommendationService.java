/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.recommendations;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.plugin.insights.core.rules.RecommendationRule;
import org.opensearch.plugin.insights.core.rules.TermOnTextFieldRule;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.transport.client.Client;

/**
 * Service that manages query recommendations
 *
 * This service:
 * 1. Maintains a registry of recommendation rules
 * 2. Evaluates rules against query records
 * 3. Stores generated recommendations
 * 4. Provides APIs to retrieve recommendations
 */
public class RecommendationService extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(RecommendationService.class);
    private static final String RECOMMENDATIONS_INDEX = "query_recommendations";
    private static final String RECOMMENDATIONS_MAPPING = "mappings/query-recommendations.json";
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final String DEFAULT_AUTO_EXPAND_REPLICAS = "0-2";

    private final ClusterService clusterService;
    private final Client client;
    private final List<RecommendationRule> rules;
    private final Map<String, List<Recommendation>> recommendationStore;

    // Two-window storage pattern (similar to TopQueriesService)
    // Current window: recommendations being accumulated during active window
    private final Map<String, Recommendation> currentWindowRecommendations;
    // Previous window: snapshot of last completed window (for fast in-memory access)
    private final Map<String, Recommendation> previousWindowRecommendations;

    private volatile boolean enabled;
    private volatile double minConfidence;
    private volatile int maxCount;
    private volatile Set<String> enabledRules;
    private volatile boolean indexCreated;

    /**
     * Constructor
     * @param clusterService the cluster service
     * @param client the client for index operations
     */
    public RecommendationService(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
        this.rules = new ArrayList<>();
        this.recommendationStore = new ConcurrentHashMap<>();
        this.currentWindowRecommendations = new ConcurrentHashMap<>();
        this.previousWindowRecommendations = new ConcurrentHashMap<>();

        // Initialize settings from cluster
        this.enabled = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_ENABLED);
        this.minConfidence = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_MIN_CONFIDENCE);
        this.maxCount = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_MAX_COUNT);
        this.enabledRules = new HashSet<>(clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_ENABLED_RULES));

        // Register settings update listeners
        clusterService.getClusterSettings().addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_ENABLED, this::setEnabled);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_MIN_CONFIDENCE, this::setMinConfidence);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_MAX_COUNT, this::setMaxCount);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_ENABLED_RULES, this::setEnabledRules);

        // Register built-in rules
        registerRule(new TermOnTextFieldRule());

        log.info("RecommendationService initialized with {} rules", rules.size());
    }

    /**
     * Register a recommendation rule
     * @param rule the rule to register
     */
    public void registerRule(RecommendationRule rule) {
        if (rule != null) {
            rules.add(rule);
            // Sort rules by priority (lower values = higher priority)
            rules.sort(Comparator.comparingInt(RecommendationRule::getPriority));
            log.info("Registered recommendation rule: {}", rule.getName());
        }
    }

    /**
     * Analyze a query record and generate recommendations
     * @param record the query record
     * @return list of recommendations
     */
    public List<Recommendation> analyzeQuery(SearchQueryRecord record) {
        if (!enabled || record == null) {
            return new ArrayList<>();
        }

        try {
            // Check if SearchSourceBuilder is available
            if (record.getSearchSourceBuilder() == null) {
                log.warn(
                    "SearchSourceBuilder is null for record {}, skipping recommendation analysis. "
                        + "This is expected behavior - SearchSourceBuilder is only available for in-memory records, not after serialization.",
                    record.getId()
                );
                return new ArrayList<>();
            }

            log.info("Analyzing query record {} for recommendations", record.getId());

            // Build query context
            QueryContext context = new QueryContext(record, clusterService);

            // Evaluate all rules
            List<Recommendation> recommendations = new ArrayList<>();
            for (RecommendationRule rule : rules) {
                try {
                    // Skip if rule is not enabled via settings
                    if (!isRuleEnabled(rule)) {
                        continue;
                    }

                    if (rule.isEnabled() && rule.matches(context)) {
                        Recommendation recommendation = rule.generate(context);
                        if (recommendation != null) {
                            recommendations.add(recommendation);
                            log.debug("Rule {} generated recommendation for query {}", rule.getId(), record.getId());
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error evaluating rule {} for query {}", rule.getId(), record.getId(), e);
                }
            }

            // Filter and limit recommendations based on settings
            recommendations = filterRecommendations(recommendations);

            // Store recommendations and get hashes
            if (!recommendations.isEmpty()) {
                // Store full recommendations in memory for immediate access
                String queryHash = record.getId();
                recommendationStore.put(queryHash, recommendations);

                // Also store recommendations by content hash and save hashes in record
                List<String> recHashes = recommendations.stream().map(this::storeRecommendationByHash).collect(Collectors.toList());

                // Store hashes in the record (this will be persisted to index)
                record.getAttributes().put(org.opensearch.plugin.insights.rules.model.Attribute.RECOMMENDATIONS, recHashes);

                log.info("Stored {} recommendations for query {} with hashes: {}", recommendations.size(), queryHash, recHashes);
            } else {
                log.info("No recommendations generated for query {}", record.getId());
            }

            return recommendations;
        } catch (Exception e) {
            log.error("Error analyzing query {}", record.getId(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Analyze multiple query records
     * @param records the query records
     * @return map of record ID to recommendations
     */
    public Map<String, List<Recommendation>> analyzeQueries(List<SearchQueryRecord> records) {
        if (!enabled || records == null || records.isEmpty()) {
            return Map.of();
        }

        return records.parallelStream().collect(Collectors.toConcurrentMap(SearchQueryRecord::getId, this::analyzeQuery));
    }

    /**
     * Get recommendations for a specific query
     * @param queryHash the query hash or record ID
     * @return list of recommendations, or empty list if not found
     */
    public List<Recommendation> getRecommendations(String queryHash) {
        return recommendationStore.getOrDefault(queryHash, new ArrayList<>());
    }

    /**
     * Clear stored recommendations
     */
    public void clearRecommendations() {
        recommendationStore.clear();
        log.info("Cleared all stored recommendations");
    }

    /**
     * Clear recommendations for a specific query
     * @param queryHash the query hash or record ID
     */
    public void clearRecommendations(String queryHash) {
        recommendationStore.remove(queryHash);
    }

    /**
     * Get all registered rules
     * @return list of rules
     */
    public List<RecommendationRule> getRules() {
        return new ArrayList<>(rules);
    }

    /**
     * Get enabled status
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set enabled status
     * @param enabled true to enable, false to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("RecommendationService enabled: {}", enabled);
    }

    /**
     * Set minimum confidence threshold
     * @param minConfidence the minimum confidence (0.0 to 1.0)
     */
    public void setMinConfidence(double minConfidence) {
        this.minConfidence = minConfidence;
        log.info("RecommendationService min confidence: {}", minConfidence);
    }

    /**
     * Set maximum count of recommendations to return
     * @param maxCount the maximum count
     */
    public void setMaxCount(int maxCount) {
        this.maxCount = maxCount;
        log.info("RecommendationService max count: {}", maxCount);
    }

    /**
     * Set enabled rules
     * @param enabledRulesList list of enabled rule IDs
     */
    public void setEnabledRules(List<String> enabledRulesList) {
        this.enabledRules = new HashSet<>(enabledRulesList);
        log.info("RecommendationService enabled rules: {}", this.enabledRules);
    }

    /**
     * Check if a rule is enabled via settings
     * @param rule the rule to check
     * @return true if enabled
     */
    private boolean isRuleEnabled(RecommendationRule rule) {
        // If enabled rules list is empty, all rules are enabled
        if (enabledRules.isEmpty()) {
            return true;
        }
        return enabledRules.contains(rule.getId());
    }

    /**
     * Filter recommendations based on settings
     * @param recommendations the list of recommendations
     * @return filtered list
     */
    private List<Recommendation> filterRecommendations(List<Recommendation> recommendations) {
        return recommendations.stream()
            .filter(r -> r.getConfidence() >= minConfidence)
            .sorted((r1, r2) -> Double.compare(r2.getConfidence(), r1.getConfidence())) // Sort by confidence desc
            .limit(maxCount)
            .collect(Collectors.toList());
    }

    /**
     * Get the size of the recommendation store
     * @return the number of queries with stored recommendations
     */
    public int getStoreSize() {
        return recommendationStore.size();
    }

    /**
     * Compute a content hash for a recommendation
     * Hash is based on content only (ruleId + title + metadata), NOT the UUID
     * This ensures identical recommendations get the same hash for deduplication
     * @param recommendation the recommendation
     * @return the hash string (e.g., "rec_a1b2c3d4e5f6")
     */
    private String computeRecommendationHash(Recommendation recommendation) {
        try {
            // Build content string for hashing (exclude UUID)
            // Include: ruleId, title, description, type, action, impact, metadata
            StringBuilder content = new StringBuilder();
            content.append("ruleId:").append(recommendation.getRuleId()).append("|");
            content.append("title:").append(recommendation.getTitle()).append("|");
            content.append("description:").append(recommendation.getDescription()).append("|");
            content.append("type:").append(recommendation.getType()).append("|");
            content.append("confidence:").append(recommendation.getConfidence()).append("|");

            // Serialize action and impact to JSON for consistent hashing
            if (recommendation.getAction() != null) {
                content.append("action:").append(Strings.toString(MediaTypeRegistry.JSON, recommendation.getAction())).append("|");
            }
            if (recommendation.getImpact() != null) {
                content.append("impact:").append(Strings.toString(MediaTypeRegistry.JSON, recommendation.getImpact())).append("|");
            }
            if (recommendation.getMetadata() != null && !recommendation.getMetadata().isEmpty()) {
                content.append("metadata:").append(recommendation.getMetadata().toString());
            }

            // Compute SHA-256 hash
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(content.toString().getBytes(StandardCharsets.UTF_8));

            // Convert to hex string (first 16 chars for readability)
            StringBuilder hexString = new StringBuilder();
            for (int i = 0; i < Math.min(8, hashBytes.length); i++) {
                String hex = Integer.toHexString(0xff & hashBytes[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return "rec_" + hexString.toString();
        } catch (Exception e) {
            log.error("Failed to compute recommendation hash", e);
            // Fallback: hash the ruleId + metadata only
            String fallback = recommendation.getRuleId() + "_" + recommendation.getMetadata().toString();
            return "rec_" + Integer.toHexString(fallback.hashCode());
        }
    }

    /**
     * Store a recommendation in memory and return its hash
     * This method is idempotent - same recommendation always produces same hash
     * Recommendations are batched in memory and bulk-exported during window rotation
     * @param recommendation the recommendation to store
     * @return the hash of the recommendation
     */
    public String storeRecommendationByHash(Recommendation recommendation) {
        String hash = computeRecommendationHash(recommendation);

        // Store in current window (putIfAbsent ensures thread-safety)
        // Deduplication happens automatically - identical recommendations share same hash
        currentWindowRecommendations.putIfAbsent(hash, recommendation);

        log.debug("Stored recommendation with hash: {} (current window)", hash);
        return hash;
    }

    /**
     * Check if the recommendations index exists
     * Reuses pattern from LocalIndexExporter
     * @return true if exists
     */
    private boolean checkRecommendationsIndexExists() {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(RECOMMENDATIONS_INDEX);
    }

    /**
     * Read index mappings from resource file
     * Reuses pattern from LocalIndexExporter.readIndexMappings()
     * @return String containing the index mappings
     * @throws IOException If there's an error reading the mappings
     */
    private String readRecommendationsMappings() throws IOException {
        return new String(
            Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(RECOMMENDATIONS_MAPPING)).readAllBytes(),
            Charset.defaultCharset()
        );
    }

    /**
     * Create the query_recommendations index if it doesn't exist
     * Reuses pattern from LocalIndexExporter.createIndexAndBulk()
     */
    private void ensureRecommendationsIndexExists() {
        if (indexCreated || checkRecommendationsIndexExists()) {
            return;
        }

        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(RECOMMENDATIONS_INDEX);

            createIndexRequest.settings(
                Settings.builder()
                    .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                    .put("index.auto_expand_replicas", DEFAULT_AUTO_EXPAND_REPLICAS)
            );
            createIndexRequest.mapping(readRecommendationsMappings());

            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    if (createIndexResponse.isAcknowledged()) {
                        indexCreated = true;
                        log.info("Created query_recommendations index successfully");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        indexCreated = true;
                        log.info("query_recommendations index already exists");
                    } else {
                        log.error("Failed to create query_recommendations index", e);
                    }
                }
            });
        } catch (IOException e) {
            log.error("Failed to read recommendations mappings", e);
        }
    }

    /**
     * Persist a recommendation to the query_recommendations index
     * @param hash the recommendation hash
     * @param recommendation the recommendation object
     */
    private void persistRecommendationToIndex(String hash, Recommendation recommendation) {
        // Ensure index exists before writing
        ensureRecommendationsIndexExists();

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("rec_hash", hash);
            builder.field("rule_id", recommendation.getRuleId());
            builder.field("first_seen", System.currentTimeMillis());
            builder.field("last_seen", System.currentTimeMillis());
            builder.field("reference_count", 1);
            builder.field("recommendation");
            recommendation.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            IndexRequest indexRequest = new IndexRequest(RECOMMENDATIONS_INDEX).id(hash)
                .source(builder)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            client.index(indexRequest);
            log.debug("Persisted recommendation {} to index", hash);
        } catch (Exception e) {
            log.error("Failed to persist recommendation {} to index", hash, e);
        }
    }

    /**
     * Get a recommendation by its hash
     * @param hash the recommendation hash
     * @return the recommendation, or null if not found
     */
    public Recommendation getRecommendationByHash(String hash) {
        // Try current window first (most likely)
        Recommendation rec = currentWindowRecommendations.get(hash);
        if (rec != null) {
            return rec;
        }

        // Try previous window (for queries in last completed window)
        rec = previousWindowRecommendations.get(hash);
        if (rec != null) {
            return rec;
        }

        // Fallback to index lookup for older data
        log.info("Recommendation hash {} not found in current/previous windows, fetching from index", hash);
        rec = loadRecommendationFromIndex(hash);
        if (rec != null) {
            // Cache in current window for future lookups
            currentWindowRecommendations.put(hash, rec);
            log.info("Loaded recommendation {} from index and cached in current window", hash);
        } else {
            log.warn("Recommendation hash {} not found in index either", hash);
        }
        return rec;
    }

    /**
     * Load a recommendation from the query_recommendations index
     * @param hash the recommendation hash
     * @return the recommendation, or null if not found
     */
    private Recommendation loadRecommendationFromIndex(String hash) {
        try {
            GetRequest getRequest = new GetRequest(RECOMMENDATIONS_INDEX, hash);
            GetResponse getResponse = client.get(getRequest).actionGet();

            if (!getResponse.isExists()) {
                return null;
            }

            Map<String, Object> source = getResponse.getSourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> recMap = (Map<String, Object>) source.get("recommendation");

            if (recMap == null) {
                log.warn("No recommendation object found in document {}", hash);
                return null;
            }

            // Reconstruct Recommendation from map
            // Parse the nested recommendation object with proper field names
            @SuppressWarnings("unchecked")
            Map<String, Object> actionMap = (Map<String, Object>) recMap.get("action");
            @SuppressWarnings("unchecked")
            Map<String, Object> impactMap = (Map<String, Object>) recMap.get("impact");
            @SuppressWarnings("unchecked")
            Map<String, Object> metadataMap = (Map<String, Object>) recMap.get("metadata");

            Recommendation.Builder builder = Recommendation.builder()
                .id((String) recMap.get("id"))
                .ruleId((String) recMap.get("rule_id"))  // Use "rule_id" with underscore
                .title((String) recMap.get("title"))
                .description((String) recMap.get("description"))
                .type(
                    org.opensearch.plugin.insights.rules.model.recommendations.RecommendationType.valueOf(
                        ((String) recMap.get("type")).toUpperCase(java.util.Locale.ROOT)
                    )
                )
                .confidence(((Number) recMap.get("confidence")).doubleValue());

            if (metadataMap != null) {
                builder.metadata(metadataMap);
            }

            // Reconstruct Action if present
            if (actionMap != null) {
                org.opensearch.plugin.insights.rules.model.recommendations.Action action =
                    org.opensearch.plugin.insights.rules.model.recommendations.Action.builder()
                        .type((String) actionMap.get("type"))
                        .hint((String) actionMap.get("hint"))
                        .codeExample((String) actionMap.get("code_example"))
                        .documentation((String) actionMap.get("documentation"))
                        .build();
                builder.action(action);
            }

            // Reconstruct ImpactVector if present
            if (impactMap != null) {
                org.opensearch.plugin.insights.rules.model.recommendations.ImpactVector impact =
                    org.opensearch.plugin.insights.rules.model.recommendations.ImpactVector.builder()
                        .latency(parseDirection((String) impactMap.get("latency")))
                        .cpu(parseDirection((String) impactMap.get("cpu")))
                        .memory(parseDirection((String) impactMap.get("memory")))
                        .correctness(parseDirection((String) impactMap.get("correctness")))
                        .confidence(((Number) impactMap.get("confidence")).doubleValue())
                        .estimatedImprovement((String) impactMap.get("estimated_improvement"))
                        .build();
                builder.impact(impact);
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error loading recommendation {} from index", hash, e);
            return null;
        }
    }

    /**
     * Parse direction string to Direction enum
     * @param directionStr the direction string (e.g., "increase", "decrease", "neutral")
     * @return the Direction enum value
     */
    private org.opensearch.plugin.insights.rules.model.recommendations.Direction parseDirection(String directionStr) {
        if (directionStr == null) {
            return org.opensearch.plugin.insights.rules.model.recommendations.Direction.NEUTRAL;
        }
        try {
            return org.opensearch.plugin.insights.rules.model.recommendations.Direction.valueOf(directionStr.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            log.warn("Unknown direction value: {}, defaulting to NEUTRAL", directionStr);
            return org.opensearch.plugin.insights.rules.model.recommendations.Direction.NEUTRAL;
        }
    }

    /**
     * Get multiple recommendations by their hashes
     * @param hashes list of recommendation hashes
     * @return list of recommendations (nulls filtered out)
     */
    public List<Recommendation> getRecommendationsByHashes(List<String> hashes) {
        return hashes.stream().map(this::getRecommendationByHash).filter(rec -> rec != null).collect(Collectors.toList());
    }

    /**
     * Get the size of the hash-based recommendation store
     * @return the number of unique recommendations stored (current + previous windows)
     */
    public int getHashStoreSize() {
        return currentWindowRecommendations.size() + previousWindowRecommendations.size();
    }

    /**
     * Export current window recommendations to index and rotate windows
     * This is called during window rotation to bulk-persist recommendations
     * Similar to how TopQueriesService exports queries on window rotation
     *
     * Window rotation pattern (same as top queries):
     * 1. Rotate: previousWindow = currentWindow (snapshot for fast access)
     * 2. Export: Bulk export previousWindow to index
     * 3. Clear: currentWindow.clear() (ready for new window)
     */
    public void exportRecommendations() {
        if (currentWindowRecommendations.isEmpty()) {
            log.debug("No recommendations in current window to export");
            return;
        }

        try {
            // Step 1: Rotate windows (snapshot current as previous)
            // Make a copy to avoid concurrent modification during export
            Map<String, Recommendation> toExport = new ConcurrentHashMap<>(currentWindowRecommendations);

            // Step 2: Clear current window for new data
            int currentSize = currentWindowRecommendations.size();
            currentWindowRecommendations.clear();
            log.info("Rotated recommendation windows: {} recommendations moved to export, current window cleared", currentSize);

            // Step 3: Bulk export to index
            ensureRecommendationsIndexExists();

            org.opensearch.action.bulk.BulkRequest bulkRequest = new org.opensearch.action.bulk.BulkRequest();
            long currentTimeMillis = System.currentTimeMillis();

            for (Map.Entry<String, Recommendation> entry : toExport.entrySet()) {
                String hash = entry.getKey();
                Recommendation recommendation = entry.getValue();

                try {
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    builder.field("rec_hash", hash);
                    builder.field("rule_id", recommendation.getRuleId());
                    builder.field("first_seen", currentTimeMillis);
                    builder.field("last_seen", currentTimeMillis);
                    builder.field("reference_count", 1);
                    builder.field("recommendation");
                    recommendation.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();

                    IndexRequest indexRequest = new IndexRequest(RECOMMENDATIONS_INDEX).id(hash).source(builder);
                    bulkRequest.add(indexRequest);
                } catch (Exception e) {
                    log.error("Failed to build index request for recommendation {}", hash, e);
                }
            }

            if (bulkRequest.numberOfActions() > 0) {
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                log.info("Bulk exporting {} recommendations to index", bulkRequest.numberOfActions());

                // Capture toExport in lambda for the callback
                final Map<String, Recommendation> exportedRecommendations = toExport;

                client.bulk(bulkRequest, new org.opensearch.core.action.ActionListener<org.opensearch.action.bulk.BulkResponse>() {
                    @Override
                    public void onResponse(org.opensearch.action.bulk.BulkResponse bulkResponse) {
                        if (bulkResponse.hasFailures()) {
                            log.warn("Bulk export of recommendations completed with failures: {}", bulkResponse.buildFailureMessage());
                        } else {
                            log.info("Successfully exported {} recommendations to index", bulkResponse.getItems().length);
                        }

                        // Step 4: Set previous window snapshot AFTER successful export
                        previousWindowRecommendations.clear();
                        previousWindowRecommendations.putAll(exportedRecommendations);
                        log.info("Updated previous window snapshot with {} recommendations", previousWindowRecommendations.size());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Failed to bulk export recommendations to index", e);
                        // On failure, put recommendations back into current window for retry
                        currentWindowRecommendations.putAll(exportedRecommendations);
                        log.warn("Export failed, restored {} recommendations to current window for retry", exportedRecommendations.size());
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error during recommendation export", e);
        }
    }

    /**
     * Delete expired recommendations based on retention period
     * Deletes recommendations where last_seen is older than deleteAfter days
     * Called by LocalIndexLifecycleManager when managing top_queries retention
     *
     * @param deleteAfter number of days to retain recommendations
     */
    public void deleteExpiredRecommendations(int deleteAfter) {
        try {
            // Calculate cutoff timestamp
            // For deleteAfter=0, use tomorrow (delete everything)
            // For deleteAfter>0, use (start of today UTC - deleteAfter days)
            long cutoffMillis;
            if (deleteAfter == 0) {
                // Delete everything - use tomorrow's date
                cutoffMillis = LocalDateTime.now(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.DAYS)
                    .plusDays(1)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
            } else {
                cutoffMillis = LocalDateTime.now(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.DAYS)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli() - TimeUnit.DAYS.toMillis(deleteAfter);
            }

            log.info("Deleting recommendations with last_seen older than {} (deleteAfter={} days)",
                new java.util.Date(cutoffMillis), deleteAfter);

            // Delete by query where last_seen < cutoffMillis
            DeleteByQueryRequest request = new DeleteByQueryRequest(RECOMMENDATIONS_INDEX);
            request.setQuery(QueryBuilders.rangeQuery("last_seen").lt(cutoffMillis));
            request.setRefresh(true);

            client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                response -> {
                    long deleted = response.getDeleted();
                    log.info("Deleted {} expired recommendations from index (older than {} days)", deleted, deleteAfter);
                    // Note: In-memory cache is NOT cleared here - it contains current window's un-exported recommendations
                    // Cache is cleared on window rotation after export, not during GC
                },
                e -> log.error("Failed to delete expired recommendations", e)
            ));
        } catch (Exception e) {
            log.error("Error during recommendation retention cleanup", e);
        }
    }

    @Override
    protected void doStart() {
        log.info("Starting RecommendationService with {} rules", rules.size());
    }

    @Override
    protected void doStop() {
        log.info("Stopping RecommendationService");
    }

    @Override
    protected void doClose() {
        clearRecommendations();
        log.info("Closed RecommendationService");
    }
}
