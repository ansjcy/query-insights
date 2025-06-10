/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Helper class containing common utility methods for Query Insights integration tests.
 * This class consolidates frequently used patterns across multiple IT files to reduce code duplication.
 */
public final class QueryInsightsTestHelper {

    private QueryInsightsTestHelper() {
        // Utility class
    }

    // ========================================
    // Common Test Data Generation Methods
    // ========================================

    /**
     * Create index with documents for testing
     */
    public static void createIndexWithData(RestClient client, String indexName, int documentCount) throws IOException {
        // Create index
        Request createIndexReq = new Request("PUT", "/" + indexName);
        createIndexReq.setJsonEntity(getDefaultIndexSettings());
        client.performRequest(createIndexReq);

        // Add documents
        for (int i = 0; i < documentCount; i++) {
            Request docReq = new Request("POST", "/" + indexName + "/_doc");
            docReq.setJsonEntity(generateTestDocument(i));
            client.performRequest(docReq);
        }

        // Refresh index
        Request refreshReq = new Request("POST", "/" + indexName + "/_refresh");
        client.performRequest(refreshReq);
    }

    /**
     * Generate test document JSON
     */
    public static String generateTestDocument(int id) {
        return "{\n"
            + "  \"id\": " + id + ",\n"
            + "  \"title\": \"Test Document " + id + "\",\n"
            + "  \"content\": \"This is test content for document " + id + "\",\n"
            + "  \"category\": \"" + (id % 3 == 0 ? "A" : id % 3 == 1 ? "B" : "C") + "\",\n"
            + "  \"value\": " + (id * 10) + ",\n"
            + "  \"timestamp\": \"2024-01-" + String.format("%02d", (id % 28) + 1) + "T10:00:00Z\"\n"
            + "}";
    }

    /**
     * Get default index settings for consistent test setup
     */
    public static String getDefaultIndexSettings() {
        return "{\n"
            + "  \"settings\": {\n"
            + "    \"index.number_of_shards\": 1,\n"
            + "    \"index.auto_expand_replicas\": \"0-2\"\n"
            + "  }\n"
            + "}";
    }

    // ========================================
    // Common Search Operations
    // ========================================

    /**
     * Execute concurrent search operations
     */
    public static void executeConcurrentSearches(RestClient client, String indexName, int threadCount, int searchesPerThread) 
            throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < searchesPerThread; j++) {
                        executeVariedSearch(client, indexName, threadId, j);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Search failed in thread " + threadId, e);
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }

    /**
     * Execute a varied search to generate different query patterns
     */
    public static void executeVariedSearch(RestClient client, String indexName, int threadId, int searchId) throws IOException {
        String queryType = getQueryTypeForId(threadId + searchId);
        String searchBody = generateSearchBody(queryType, threadId, searchId);
        
        Request searchReq = new Request("GET", "/" + indexName + "/_search");
        searchReq.setJsonEntity(searchBody);
        
        Response response = client.performRequest(searchReq);
        Assert.assertEquals("Search should succeed", 200, response.getStatusLine().getStatusCode());
    }

    /**
     * Get query type based on ID for varied patterns
     */
    private static String getQueryTypeForId(int id) {
        String[] queryTypes = {"match", "term", "range", "bool", "wildcard"};
        return queryTypes[id % queryTypes.length];
    }

    /**
     * Generate search body for different query types
     */
    public static String generateSearchBody(String queryType, int threadId, int searchId) {
        switch (queryType) {
            case "match":
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"match\": {\n"
                    + "      \"title\": \"Test Document " + (searchId % 10) + "\"\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"size\": " + (10 + threadId) + "\n"
                    + "}";
            
            case "term":
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"term\": {\n"
                    + "      \"category.keyword\": \"" + (searchId % 3 == 0 ? "A" : searchId % 3 == 1 ? "B" : "C") + "\"\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"size\": " + (5 + threadId) + "\n"
                    + "}";
            
            case "range":
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"range\": {\n"
                    + "      \"value\": {\n"
                    + "        \"gte\": " + (searchId * 5) + ",\n"
                    + "        \"lte\": " + ((searchId + 10) * 5) + "\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"size\": " + (15 + threadId) + "\n"
                    + "}";
            
            case "bool":
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"bool\": {\n"
                    + "      \"must\": [\n"
                    + "        {\n"
                    + "          \"match\": {\n"
                    + "            \"content\": \"test\"\n"
                    + "          }\n"
                    + "        }\n"
                    + "      ],\n"
                    + "      \"filter\": [\n"
                    + "        {\n"
                    + "          \"range\": {\n"
                    + "            \"id\": {\n"
                    + "              \"gte\": " + (threadId * 10) + "\n"
                    + "            }\n"
                    + "          }\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"size\": " + (20 + threadId) + "\n"
                    + "}";
            
            case "wildcard":
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"wildcard\": {\n"
                    + "      \"title.keyword\": \"Test*" + (searchId % 5) + "\"\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"size\": " + (8 + threadId) + "\n"
                    + "}";
            
            default:
                return "{\n"
                    + "  \"query\": {\n"
                    + "    \"match_all\": {}\n"
                    + "  },\n"
                    + "  \"size\": " + (10 + threadId) + "\n"
                    + "}";
        }
    }

    // ========================================
    // Common Validation Methods
    // ========================================

    /**
     * Validate that response contains expected structure and data
     */
    public static void validateTopQueriesResponse(String response, String expectedMetricType) throws IOException {
        Assert.assertNotNull("Response should not be null", response);
        Assert.assertTrue("Response should contain top_queries", response.contains("top_queries"));
        
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();
            Assert.assertTrue("Response should have top_queries field", responseMap.containsKey("top_queries"));
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> topQueries = (List<Map<String, Object>>) responseMap.get("top_queries");
            
            if (topQueries != null && !topQueries.isEmpty()) {
                for (Map<String, Object> query : topQueries) {
                    validateQueryStructure(query, expectedMetricType);
                }
            }
        }
    }

    /**
     * Validate individual query structure
     */
    private static void validateQueryStructure(Map<String, Object> query, String expectedMetricType) {
        Assert.assertTrue("Query should have timestamp", query.containsKey("timestamp"));
        Assert.assertTrue("Query should have measurements", query.containsKey("measurements"));
        Assert.assertTrue("Query should have node_id", query.containsKey("node_id"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> measurements = (Map<String, Object>) query.get("measurements");
        Assert.assertNotNull("Measurements should not be null", measurements);
        
        if (expectedMetricType != null) {
            Assert.assertTrue(
                "Measurements should contain " + expectedMetricType, 
                measurements.containsKey(expectedMetricType)
            );
        }
    }

    /**
     * Validate cluster health status
     */
    public static void validateClusterHealth(RestClient client) throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        Response response = client.performRequest(request);
        Assert.assertEquals("Cluster health check should succeed", 200, response.getStatusLine().getStatusCode());
        
        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        Assert.assertTrue("Cluster should be healthy", 
            responseBody.contains("\"status\":\"green\"") || responseBody.contains("\"status\":\"yellow\""));
    }

    /**
     * Get cluster statistics for memory/performance monitoring
     */
    public static Map<String, Object> getClusterStats(RestClient client) throws IOException {
        Request request = new Request("GET", "/_cluster/stats");
        Response response = client.performRequest(request);
        Assert.assertEquals("Cluster stats should be available", 200, response.getStatusLine().getStatusCode());
        
        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            return parser.map();
        }
    }
}
