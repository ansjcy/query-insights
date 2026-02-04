/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.recommendations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Context object that provides query information and metadata for rule evaluation
 */
public class QueryContext {
    private static final Logger log = LogManager.getLogger(QueryContext.class);

    private final SearchQueryRecord record;
    private final SearchSourceBuilder searchSourceBuilder;
    private final ClusterService clusterService;

    /**
     * Constructor for QueryContext
     * @param record the search query record
     * @param clusterService the cluster service for metadata access
     */
    public QueryContext(SearchQueryRecord record, ClusterService clusterService) {
        this(record, record.getSearchSourceBuilder(), clusterService);
    }

    /**
     * Constructor for QueryContext with explicit SearchSourceBuilder
     * @param record the search query record
     * @param searchSourceBuilder the search source builder (may be reconstructed from SOURCE attribute)
     * @param clusterService the cluster service for metadata access
     */
    public QueryContext(SearchQueryRecord record, SearchSourceBuilder searchSourceBuilder, ClusterService clusterService) {
        this.record = Objects.requireNonNull(record, "record must not be null");
        this.clusterService = clusterService;
        this.searchSourceBuilder = searchSourceBuilder;
    }

    /**
     * Get the search query record
     * @return the record
     */
    public SearchQueryRecord getRecord() {
        return record;
    }

    /**
     * Get the search source builder
     * @return the search source builder, or null if not available
     */
    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }

    /**
     * Get the cluster service
     * @return the cluster service
     */
    public ClusterService getClusterService() {
        return clusterService;
    }

    /**
     * Get the indices being queried
     * @return the list of indices, or empty list if not available
     */
    @SuppressWarnings("unchecked")
    public List<String> getIndices() {
        Object indicesObj = record.getAttributes().get(Attribute.INDICES);
        log.info(
            "QueryContext.getIndices: INDICES attribute value: {}, type: {}",
            indicesObj,
            indicesObj != null ? indicesObj.getClass().getSimpleName() : "null"
        );

        // Handle List type
        if (indicesObj instanceof List) {
            List<String> indices = (List<String>) indicesObj;
            log.info("QueryContext.getIndices: Returning list of size: {}, contents: {}", indices.size(), indices);
            return indices;
        }

        // Handle String[] array type (from tracked queries)
        if (indicesObj instanceof String[]) {
            List<String> indices = java.util.Arrays.asList((String[]) indicesObj);
            log.info("QueryContext.getIndices: Converted String[] to list of size: {}, contents: {}", indices.size(), indices);
            return indices;
        }

        log.warn("QueryContext.getIndices: INDICES attribute is neither List nor String[], returning empty list");
        return new ArrayList<>();
    }

    /**
     * Extract term query builders from the query
     * This performs a recursive search through the query tree
     * @return list of term query builders
     */
    public List<TermQueryBuilder> extractTermQueries() {
        List<TermQueryBuilder> termQueries = new ArrayList<>();
        if (searchSourceBuilder != null && searchSourceBuilder.query() != null) {
            extractTermQueriesRecursive(searchSourceBuilder.query(), termQueries);
        }
        return termQueries;
    }

    /**
     * Extract terms query builders from the query
     * @return list of terms query builders
     */
    public List<TermsQueryBuilder> extractTermsQueries() {
        List<TermsQueryBuilder> termsQueries = new ArrayList<>();
        if (searchSourceBuilder != null && searchSourceBuilder.query() != null) {
            extractTermsQueriesRecursive(searchSourceBuilder.query(), termsQueries);
        }
        return termsQueries;
    }

    /**
     * Extract wildcard query builders from the query
     * @return list of wildcard query builders
     */
    public List<WildcardQueryBuilder> extractWildcardQueries() {
        List<WildcardQueryBuilder> wildcardQueries = new ArrayList<>();
        if (searchSourceBuilder != null && searchSourceBuilder.query() != null) {
            extractWildcardQueriesRecursive(searchSourceBuilder.query(), wildcardQueries);
        }
        return wildcardQueries;
    }

    /**
     * Get the field type for a given field from the index mapping
     * @param fieldName the field name
     * @return the field type, or null if not found
     */
    public String getFieldType(String fieldName) {
        if (clusterService == null) {
            log.warn("ClusterService is null, cannot get field type for: {}", fieldName);
            return null;
        }

        List<String> indices = getIndices();
        log.info("QueryContext.getFieldType: indices list size: {}, indices: {}", indices.size(), indices);
        if (indices.isEmpty()) {
            log.warn("No indices found in query context, cannot determine field type for: {}", fieldName);
            return null;
        }

        try {
            // Check the first index for the field type
            // In a real implementation, we might want to check all indices
            String indexName = indices.get(0);
            log.info("QueryContext.getFieldType: Looking up field {} in index {}", fieldName, indexName);
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
            if (indexMetadata != null) {
                MappingMetadata mappingMetadata = indexMetadata.mapping();
                if (mappingMetadata != null) {
                    Map<String, Object> sourceAsMap = mappingMetadata.sourceAsMap();
                    String fieldType = getFieldTypeFromMapping(fieldName, sourceAsMap);
                    log.info("QueryContext.getFieldType: Field {} in index {} has type: {}", fieldName, indexName, fieldType);
                    return fieldType;
                } else {
                    log.warn("MappingMetadata is null for index: {}", indexName);
                }
            } else {
                log.warn("IndexMetadata is null for index: {}", indexName);
            }
        } catch (Exception e) {
            log.warn("Error getting field type for field: {}", fieldName, e);
        }

        return null;
    }

    /**
     * Check if a field has a keyword subfield
     * @param fieldName the field name
     * @return true if the field has a .keyword subfield
     */
    public boolean hasKeywordSubfield(String fieldName) {
        String keywordFieldType = getFieldType(fieldName + ".keyword");
        return "keyword".equals(keywordFieldType);
    }

    /**
     * Get the from + size for pagination
     * @return the total pagination depth, or 0 if not available
     */
    public int getPaginationDepth() {
        if (searchSourceBuilder == null) {
            return 0;
        }
        return searchSourceBuilder.from() + searchSourceBuilder.size();
    }

    // Private helper methods

    @SuppressWarnings("unchecked")
    private String getFieldTypeFromMapping(String fieldName, Map<String, Object> mapping) {
        if (mapping == null) {
            return null;
        }

        // Navigate through properties
        Object propertiesObj = mapping.get("properties");
        if (!(propertiesObj instanceof Map)) {
            return null;
        }

        Map<String, Object> properties = (Map<String, Object>) propertiesObj;

        // Handle nested field paths (e.g., "user.name" or "title.keyword")
        String[] fieldParts = fieldName.split("\\.");
        Map<String, Object> currentLevel = properties;

        for (int i = 0; i < fieldParts.length; i++) {
            String part = fieldParts[i];
            Object fieldObj = currentLevel.get(part);

            if (!(fieldObj instanceof Map)) {
                return null;
            }

            Map<String, Object> fieldMapping = (Map<String, Object>) fieldObj;

            // If this is the last part, get the type
            if (i == fieldParts.length - 1) {
                Object typeObj = fieldMapping.get("type");
                return typeObj != null ? typeObj.toString() : null;
            }

            // Navigate deeper - check for subfields
            Object fieldsObj = fieldMapping.get("fields");
            if (fieldsObj instanceof Map) {
                currentLevel = (Map<String, Object>) fieldsObj;
            } else {
                // Navigate to nested properties
                Object nestedProps = fieldMapping.get("properties");
                if (nestedProps instanceof Map) {
                    currentLevel = (Map<String, Object>) nestedProps;
                } else {
                    return null;
                }
            }
        }

        return null;
    }

    private void extractTermQueriesRecursive(QueryBuilder query, List<TermQueryBuilder> results) {
        if (query instanceof TermQueryBuilder) {
            results.add((TermQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.BoolQueryBuilder) {
            org.opensearch.index.query.BoolQueryBuilder boolQuery = (org.opensearch.index.query.BoolQueryBuilder) query;
            boolQuery.must().forEach(q -> extractTermQueriesRecursive(q, results));
            boolQuery.should().forEach(q -> extractTermQueriesRecursive(q, results));
            boolQuery.filter().forEach(q -> extractTermQueriesRecursive(q, results));
            boolQuery.mustNot().forEach(q -> extractTermQueriesRecursive(q, results));
        }
        // Add more query types as needed
    }

    private void extractTermsQueriesRecursive(QueryBuilder query, List<TermsQueryBuilder> results) {
        if (query instanceof TermsQueryBuilder) {
            results.add((TermsQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.BoolQueryBuilder) {
            org.opensearch.index.query.BoolQueryBuilder boolQuery = (org.opensearch.index.query.BoolQueryBuilder) query;
            boolQuery.must().forEach(q -> extractTermsQueriesRecursive(q, results));
            boolQuery.should().forEach(q -> extractTermsQueriesRecursive(q, results));
            boolQuery.filter().forEach(q -> extractTermsQueriesRecursive(q, results));
            boolQuery.mustNot().forEach(q -> extractTermsQueriesRecursive(q, results));
        }
    }

    private void extractWildcardQueriesRecursive(QueryBuilder query, List<WildcardQueryBuilder> results) {
        if (query instanceof WildcardQueryBuilder) {
            results.add((WildcardQueryBuilder) query);
        } else if (query instanceof org.opensearch.index.query.BoolQueryBuilder) {
            org.opensearch.index.query.BoolQueryBuilder boolQuery = (org.opensearch.index.query.BoolQueryBuilder) query;
            boolQuery.must().forEach(q -> extractWildcardQueriesRecursive(q, results));
            boolQuery.should().forEach(q -> extractWildcardQueriesRecursive(q, results));
            boolQuery.filter().forEach(q -> extractWildcardQueriesRecursive(q, results));
            boolQuery.mustNot().forEach(q -> extractWildcardQueriesRecursive(q, results));
        }
    }
}
