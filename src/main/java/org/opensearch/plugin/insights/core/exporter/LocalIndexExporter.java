/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PATTERN_GLOB;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.action.indices.PutIndexTemplateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.insights.core.metrics.OperationalMetric;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.client.Client;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import java.util.ArrayList;
import java.util.Map;

/**
 * Local index exporter for exporting query insights data to local OpenSearch indices.
 */
public class LocalIndexExporter implements QueryInsightsExporter {
    /**
     * Logger of the local index exporter
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private final ClusterService clusterService;
    private final String indexMapping;
    private DateTimeFormatter indexPattern;
    private int deleteAfter;
    private final String id;
    private static final int DEFAULT_NUMBER_OF_REPLICA = 1;
    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    private static final List<String> DEFAULT_SORTED_FIELDS = List.of(
        "measurements.latency.number",
        "measurements.cpu.number",
        "measurements.memory.number"
    );
    private static final List<String> DEFAULT_SORTED_ORDERS = List.of("desc", "desc", "desc");
    private static final String TEMPLATE_NAME = "query_insights_override";
    private int templateOrder;

    /**
     * Constructor
     *
     * @param client          client instance
     * @param clusterService  cluster service
     * @param indexPattern    index pattern
     * @param indexMapping    index mapping
     * @param id              exporter id
     */
    public LocalIndexExporter(
        final Client client,
        final ClusterService clusterService,
        final DateTimeFormatter indexPattern,
        final String indexMapping,
        final String id
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexPattern = indexPattern;
        this.indexMapping = indexMapping;
        this.id = id;
        this.deleteAfter = DEFAULT_DELETE_AFTER_VALUE;
        this.templateOrder = QueryInsightsSettings.DEFAULT_TEMPLATE_ORDER;
    }

    /**
     * Retrieves the identifier for the local index exporter.
     *
     * Each service can either have its own dedicated local index exporter or share
     * an existing one. This identifier is used by the QueryInsightsExporterFactory
     * to locate and manage the appropriate exporter instance.
     *
     * @return The identifier of the local index exporter
     * @see QueryInsightsExporterFactory
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Getter of indexPattern
     *
     * @return indexPattern
     */
    public DateTimeFormatter getIndexPattern() {
        return indexPattern;
    }

    /**
     * Setter of indexPattern
     *
     * @param indexPattern index pattern
     */
    public void setIndexPattern(DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
    }

    /**
     * Export a list of SearchQueryRecord to a local index
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        try {
            final String indexName = buildLocalIndexName();
            if (!checkIndexExists(indexName)) {
                // Check for conflicting templates once and reuse results
                TemplateConflictInfo conflictInfo = findConflictingTemplatesWithDetails();
                if (!conflictInfo.getConflictingTemplates().isEmpty()) {
                    logger.warn("Detected potentially conflicting templates: {}", conflictInfo.getConflictingTemplates());
                }
                
                // Adjust the template order if needed based on conflict info
                if (conflictInfo.getHighestOrder() > 0) {
                    int adjustedOrder = calculateAdjustedOrder(conflictInfo.getHighestOrder());
                    if (adjustedOrder > templateOrder) {
                        logger.debug("Adjusting template order from {} to {} due to conflicting templates", 
                            templateOrder, adjustedOrder);
                        templateOrder = adjustedOrder;
                    }
                }
                
                // Create template with adjusted order
                ensureTemplateExists();
                
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

                createIndexRequest.masterNodeTimeout(TimeValue.timeValueMinutes(1));
                createIndexRequest.includeTypeName(false);

                createIndexRequest.settings(
                    Settings.builder()
                        .putList("index.sort.field", DEFAULT_SORTED_FIELDS)
                        .putList("index.sort.order", DEFAULT_SORTED_ORDERS)
                        .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                        .put("index.number_of_replicas", DEFAULT_NUMBER_OF_REPLICA)
                );
                createIndexRequest.mapping(readIndexMappings());

                client.admin().indices().create(createIndexRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        if (createIndexResponse.isAcknowledged()) {
                            try {
                                bulk(indexName, records);
                            } catch (IOException e) {
                                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                                logger.error("Unable to index query insights data: ", e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof ResourceAlreadyExistsException) {
                            try {
                                bulk(indexName, records);
                            } catch (IOException ioe) {
                                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                                logger.error("Unable to index query insights data: ", ioe);
                            }
                        } else {
                            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                            
                            // Use the conflict info we already have for error reporting
                            if (cause.getMessage() != null && cause.getMessage().contains("index sort field")) {
                                logger.error("Index creation failed due to sort field issue. This is likely caused by a conflicting template. " +
                                    "Detected templates: {}. Try increasing the template order value or removing conflicting templates.", 
                                    conflictInfo.getConflictingTemplates(), cause);
                            } else {
                                logger.error("Unable to create query insights index: ", cause);
                            }
                        }
                    }
                });
            } else {
                bulk(indexName, records);
            }
        } catch (IOException e) {
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
            logger.error("Unable to create query insights exporter: ", e);
        }
    }

    private void bulk(final String indexName, final List<SearchQueryRecord> records) throws IOException {
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
        for (SearchQueryRecord record : records) {
            bulkRequestBuilder.add(
                new IndexRequest(indexName).id(record.getId())
                    .source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_BULK_FAILURES);
                logger.error("Failed to execute bulk operation for query insights data: ", e);
            }
        });
    }

    /**
     * Close the exporter sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexExporter..");
    }

    /**
     * Builds the local index name using the current UTC datetime
     *
     * @return A string representing the index name in the format "top_queries-YYYY.MM.dd-01234".
     */
    String buildLocalIndexName() {
        ZonedDateTime currentTime = ZonedDateTime.now(ZoneOffset.UTC);
        return indexPattern.format(currentTime) + "-" + generateLocalIndexDateHash(currentTime.toLocalDate());
    }

    /**
     * Set local index exporter data retention period
     *
     * @param deleteAfter the number of days after which Top N local indices should be deleted
     */
    public void setDeleteAfter(final int deleteAfter) {
        this.deleteAfter = deleteAfter;
    }

    /**
     * Get local index exporter data retention period
     *
     * @return the number of days after which Top N local indices should be deleted
     */
    public int getDeleteAfter() {
        return deleteAfter;
    }

    /**
     * Deletes the specified index and logs any failure that occurs during the operation.
     *
     * @param indexName The name of the index to delete.
     * @param client The OpenSearch client used to perform the deletion.
     */
    public void deleteSingleIndex(String indexName, Client client) {
        Logger logger = LogManager.getLogger();
        client.admin().indices().delete(new DeleteIndexRequest(indexName), new ActionListener<>() {
            @Override
            // CS-SUPPRESS-SINGLE: RegexpSingleline It is not possible to use phrase "cluster manager" instead of master here
            public void onResponse(org.opensearch.action.support.master.AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof IndexNotFoundException) {
                    return;
                }
                OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_DELETE_FAILURES);
                logger.error("Failed to delete index '{}': ", indexName, e);
            }
        });
    }

    /**
     * check if index exists
     * @return boolean
     */
    private boolean checkIndexExists(String indexName) {
        ClusterState clusterState = clusterService.state();
        return clusterState.getRoutingTable().hasIndex(indexName);
    }

    /**
     * Read index mappings from file or string content
     * @return The mapping content as a String
     * @throws IOException If there's an error reading mappings
     */
    private String readIndexMappings() throws IOException {
        if (indexMapping == null || indexMapping.isEmpty()) {
            return "{}";
        }
        
        // Check if this is a resource path or direct content
        if (indexMapping.endsWith(".json")) {
            return new String(
                Objects.requireNonNull(LocalIndexExporter.class.getClassLoader().getResourceAsStream(indexMapping)).readAllBytes(),
                Charset.defaultCharset()
            );
        }
        
        return indexMapping;
    }
    
    /**
     * Read index mappings as a Map for V2 templates
     * @return Map representation of mappings
     * @throws IOException If there's an error reading mappings
     */
    private Map<String, Object> readIndexMappingsAsMap() throws IOException {
        String mappingString = readIndexMappings();
        return org.opensearch.core.xcontent.XContentHelper.convertToMap(
            org.opensearch.core.xcontent.XContentType.JSON.xContent(), 
            mappingString, 
            false
        );
    }

    /**
     * Check if our template exists
     * @param templateName template name to check
     * @return true if template exists, false otherwise
     */
    private boolean checkTemplateExists(String templateName) {
        try {
            ClusterState state = clusterService.state();
            return state.getMetadata().templates().containsKey(templateName);
        } catch (Exception e) {
            logger.error("Error checking template existence [{}]", templateName, e);
            return false;
        }
    }

    /**
     * Ensure a template exists for our index pattern with a high priority
     * to override any wildcard templates
     */
    private void ensureTemplateExists() {
        String indexPattern = TOP_QUERIES_INDEX_PATTERN_GLOB;
        
        // First check for any templates we need to compete with and adjust order if needed
        templateOrder = getAdjustedTemplateOrder();
        
        try {
            boolean templateNeedsCreation = true;
            boolean templateNeedsUpdate = false;
            int existingPriority = -1;
            
            // Check if our template exists in V2 templates
            ClusterState state = clusterService.state();
            if (state.getMetadata().templatesV2().containsKey(TEMPLATE_NAME)) {
                templateNeedsCreation = false;
                org.opensearch.cluster.metadata.ComposableIndexTemplate existingTemplate = 
                    state.getMetadata().templatesV2().get(TEMPLATE_NAME);
                
                if (existingTemplate != null) {
                    Integer currentPriority = existingTemplate.priority();
                    existingPriority = currentPriority != null ? currentPriority : 0;
                    if (existingPriority < templateOrder) {
                        templateNeedsUpdate = true;
                        logger.debug("Existing template [{}] priority {} needs to be updated to {}", 
                            TEMPLATE_NAME, existingPriority, templateOrder);
                    }
                }
            } else {
                // Also check in V1 templates for backward compatibility
                if (state.getMetadata().templates().containsKey(TEMPLATE_NAME)) {
                    // If a V1 template exists, we'll replace it with a V2 template
                    templateNeedsCreation = false;
                    templateNeedsUpdate = true;
                    org.opensearch.cluster.metadata.IndexTemplateMetadata existingTemplate = 
                        state.getMetadata().templates().get(TEMPLATE_NAME);
                    
                    if (existingTemplate != null) {
                        Integer currentOrder = existingTemplate.order();
                        existingPriority = currentOrder != null ? currentOrder : 0;
                        logger.debug("Found old V1 template [{}] with order {}. Will replace with V2 template.", 
                            TEMPLATE_NAME, existingPriority);
                    }
                }
            }
            
            // Skip if template exists and doesn't need updating
            if (!templateNeedsCreation && !templateNeedsUpdate) {
                logger.debug("Template [{}] already exists with appropriate priority", TEMPLATE_NAME);
                return;
            }
            
            // Create a V2 template (ComposableIndexTemplate)
            String mappingContent = readIndexMappings();
            org.opensearch.core.xcontent.XContentType xContentType = org.opensearch.core.xcontent.XContentType.JSON;
            org.opensearch.cluster.metadata.CompressedXContent compressedMapping = 
                new org.opensearch.cluster.metadata.CompressedXContent(mappingContent);
                
            // Create template component
            org.opensearch.cluster.metadata.Template template = new org.opensearch.cluster.metadata.Template(
                Settings.builder()
                    .putList("index.sort.field", DEFAULT_SORTED_FIELDS)
                    .putList("index.sort.order", DEFAULT_SORTED_ORDERS)
                    .put("index.number_of_shards", DEFAULT_NUMBER_OF_SHARDS)
                    .put("index.number_of_replicas", DEFAULT_NUMBER_OF_REPLICA)
                    .build(),
                compressedMapping,
                null // No aliases
            );
            
            // Create the composable template
            org.opensearch.cluster.metadata.ComposableIndexTemplate composableTemplate = 
                new org.opensearch.cluster.metadata.ComposableIndexTemplate(
                    Collections.singletonList(indexPattern),
                    template,
                    null, // No composed_of templates
                    templateOrder, // Priority (using the same value as our adjusted order)
                    templateOrder, // Version (using order as version for tracking changes)
                    null, // No metadata
                    null // No data stream
                );
                
            // Use the V2 API to put the template
            org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request request =
                new org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request(TEMPLATE_NAME)
                    .indexTemplate(composableTemplate);
                    
            client.execute(
                org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.INSTANCE,
                request,
                new ActionListener<>() {
                    @Override
                    public void onResponse(org.opensearch.action.support.AcknowledgedResponse response) {
                        if (response.isAcknowledged()) {
                            logger.info("Successfully {} V2 template [{}] with priority {}", 
                                templateNeedsCreation ? "created" : "updated", TEMPLATE_NAME, templateOrder);
                        } else {
                            logger.warn("Failed to {} V2 template [{}]", 
                                templateNeedsCreation ? "create" : "update", TEMPLATE_NAME);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Error {} V2 template [{}]", 
                            templateNeedsCreation ? "creating" : "updating", TEMPLATE_NAME, e);
                        OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
                    }
                }
            );
        } catch (Exception e) {
            logger.error("Failed to manage V2 template", e);
            OperationalMetricsCounter.getInstance().incrementCounter(OperationalMetric.LOCAL_INDEX_EXPORTER_EXCEPTIONS);
        }
    }

    /**
     * Find templates that might conflict with our index creation and collect information about them
     * @return TemplateConflictInfo containing conflicting templates and their highest order/priority
     */
    private TemplateConflictInfo findConflictingTemplatesWithDetails() {
        List<String> conflictingTemplates = new ArrayList<>();
        int highestOrder = 0;

        try {
            ClusterState state = clusterService.state();
            String ourIndexPattern = TOP_QUERIES_INDEX_PATTERN_GLOB;
            
            // Find V1 templates that match our index pattern
            state.getMetadata().templates().forEach((name, template) -> {
                for (String pattern : template.patterns()) {
                    // Check if this template would match our index pattern
                    if (pattern.equals("*") || ourIndexPattern.matches(patternToRegex(pattern))) {
                        // Only process templates not created by us
                        if (!name.startsWith(TEMPLATE_NAME)) {
                            Integer order = template.order();
                            int orderValue = order != null ? order : 0;
                            
                            // Track the highest order value
                            if (orderValue > highestOrder) {
                                highestOrder = orderValue;
                            }
                            
                            conflictingTemplates.add(name + " (V1, pattern: " + pattern + ", order: " + 
                                (order != null ? order : "default") + ")");
                        }
                    }
                }
            });
            
            // Also check V2 templates (composable templates)
            state.getMetadata().templatesV2().forEach((name, template) -> {
                List<String> patterns = template.indexPatterns();
                for (String pattern : patterns) {
                    // Check if this template would match our index pattern
                    if (pattern.equals("*") || ourIndexPattern.matches(patternToRegex(pattern))) {
                        // Only process templates not created by us
                        if (!name.startsWith(TEMPLATE_NAME)) {
                            // For V2 templates, priority is analogous to order in V1
                            Integer priority = template.priority();
                            int priorityValue = priority != null ? priority : 0;
                            
                            // Track the highest priority value (to compare with V1 order)
                            if (priorityValue > highestOrder) {
                                highestOrder = priorityValue;
                            }
                            
                            conflictingTemplates.add(name + " (V2, pattern: " + pattern + ", priority: " + 
                                (priority != null ? priority : "default") + ")");
                        }
                    }
                }
            });
        } catch (Exception e) {
            logger.error("Error checking for conflicting templates", e);
        }
        
        return new TemplateConflictInfo(conflictingTemplates, highestOrder);
    }
    
    /**
     * Convert an index pattern to a regex for matching
     */
    private String patternToRegex(String pattern) {
        return pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");
    }

    /**
     * Find templates that might conflict with our index creation
     * @return List of potentially conflicting template names
     */
    private List<String> findConflictingTemplates() {
        return findConflictingTemplatesWithDetails().getConflictingTemplates();
    }
    
    /**
     * Ensure we're using a template order higher than any conflicting template
     * @return The adjusted template order
     */
    private int getAdjustedTemplateOrder() {
        TemplateConflictInfo conflictInfo = findConflictingTemplatesWithDetails();
        return calculateAdjustedOrder(conflictInfo.getHighestOrder());
    }
    
    /**
     * Calculate the adjusted order value based on a conflicting order
     * @param highestConflictingOrder The highest order value found in conflicting templates
     * @return The adjusted order value
     */
    private int calculateAdjustedOrder(int highestConflictingOrder) {
        // If no conflicts, return the current order
        if (highestConflictingOrder <= 0) {
            return templateOrder;
        }
        
        // Add a buffer to ensure we're substantially higher
        int adjustedOrder = highestConflictingOrder + 100;
        
        // If template order gets too high, we might approach Integer.MAX_VALUE
        // In that case, use a very high value but leave room for future conflicts
        if (adjustedOrder > Integer.MAX_VALUE - 1000) {
            adjustedOrder = Integer.MAX_VALUE - 1000;
        }
        
        // Only log if we're actually changing the order
        if (adjustedOrder > templateOrder) {
            logger.debug("Adjusting template order from {} to {} due to conflicting templates with highest order/priority {}",
                templateOrder, adjustedOrder, highestConflictingOrder);
            return adjustedOrder;
        }
        
        return templateOrder;
    }
    
    /**
     * Value class to hold information about conflicting templates
     */
    private static class TemplateConflictInfo {
        private final List<String> conflictingTemplates;
        private final int highestOrder;
        
        TemplateConflictInfo(List<String> conflictingTemplates, int highestOrder) {
            this.conflictingTemplates = conflictingTemplates;
            this.highestOrder = highestOrder;
        }
        
        List<String> getConflictingTemplates() {
            return conflictingTemplates;
        }
        
        int getHighestOrder() {
            return highestOrder;
        }
    }

    /**
     * Set the template order for the exporter
     * @param templateOrder New template order value
     */
    public void setTemplateOrder(final int templateOrder) {
        this.templateOrder = templateOrder;
    }

    /**
     * Get the current template order
     * @return Current template order value
     */
    public int getTemplateOrder() {
        return templateOrder;
    }

}
