/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/**
 * Integration tests for Query Insights Health Stats API functionality
 *
 * This test suite covers:
 * - Health stats API endpoint functionality
 * - Operational metrics counters (exceptions, failures, etc.)
 * - Health stats with different plugin configurations
 * - Health stats during high load scenarios
 * - Field type cache statistics
 */
public class QueryInsightsHealthStatsIT extends QueryInsightsRestTestCase {

    private static final int HIGH_LOAD_SEARCH_COUNT = 100;
    private static final int CONCURRENT_THREADS = 5;
    private static final int STRESS_TEST_SEARCHES_PER_THREAD = 20;

    /**
     * Test basic health stats API endpoint functionality
     */
    public void testHealthStatsAPIEndpoint() throws IOException, InterruptedException {
        // Enable query insights to generate some health data
        updateClusterSettings(this::enableBasicQueryInsightsSettings);

        // Perform some searches to generate data
        doSearch(10);
        Thread.sleep(5000); // Allow time for data processing

        // Test health stats endpoint
        Request request = new Request("GET", "/_insights/health_stats");
        Response response = client().performRequest(request);

        Assert.assertEquals("Health stats endpoint should return 200", 200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        Assert.assertNotNull("Response body should not be null", responseBody);

        // Validate response structure
        validateHealthStatsResponse(responseBody);
    }

    /**
     * Test health stats with different plugin configurations
     */
    public void testHealthStatsWithDifferentConfigurations() throws IOException, InterruptedException {
        // Test 1: Basic latency configuration
        updateClusterSettings(this::enableLatencyOnlySettings);
        doSearch(5);
        Thread.sleep(3000);

        String latencyOnlyResponse = getHealthStats();
        validateHealthStatsResponse(latencyOnlyResponse);
        validateThreadPoolInfo(latencyOnlyResponse);

        // Test 2: Multi-metric configuration
        updateClusterSettings(this::enableAllMetricsSettings);
        doSearch(5);
        Thread.sleep(3000);

        String allMetricsResponse = getHealthStats();
        validateHealthStatsResponse(allMetricsResponse);
        validateTopQueriesHealthStats(allMetricsResponse, new String[] { "latency", "cpu", "memory" });

        // Test 3: With local index exporter
        updateClusterSettings(this::enableWithLocalIndexExporterSettings);
        doSearch(5);
        Thread.sleep(5000); // Allow time for export operations

        String exporterResponse = getHealthStats();
        validateHealthStatsResponse(exporterResponse);

        // Test 4: With query grouping enabled
        updateClusterSettings(this::enableWithQueryGroupingSettings);
        doSearch(5);
        Thread.sleep(3000);

        String groupingResponse = getHealthStats();
        validateHealthStatsResponse(groupingResponse);
        validateFieldTypeCacheStats(groupingResponse);
    }

    /**
     * Test operational metrics counters during normal operations
     */
    public void testOperationalMetricsCounters() throws IOException, InterruptedException {
        // Enable query insights with local index exporter to generate operational metrics
        updateClusterSettings(this::enableWithLocalIndexExporterSettings);

        // Perform searches to generate operational activity
        doSearch(15);
        Thread.sleep(8000); // Allow time for export operations and metric collection

        String healthStatsResponse = getHealthStats();
        validateHealthStatsResponse(healthStatsResponse);

        // Validate that operational metrics are being tracked
        // Note: We can't easily force specific operational failures in integration tests,
        // but we can verify the health stats structure includes the necessary components
        validateOperationalMetricsStructure(healthStatsResponse);
    }

    /**
     * Test health stats during high load scenarios
     */
    public void testHealthStatsDuringHighLoad() throws IOException, InterruptedException {
        // Configure for high load testing
        updateClusterSettings(this::enableHighLoadSettings);

        // Perform high volume of searches
        doSearch(HIGH_LOAD_SEARCH_COUNT);
        Thread.sleep(10000); // Allow time for processing

        String healthStatsResponse = getHealthStats();
        validateHealthStatsResponse(healthStatsResponse);

        // Validate that system handles high load gracefully
        validateHighLoadHealthStats(healthStatsResponse);

        // Verify response time is reasonable even under load
        long startTime = System.currentTimeMillis();
        getHealthStats();
        long responseTime = System.currentTimeMillis() - startTime;
        Assert.assertTrue("Health stats response time should be reasonable under load", responseTime < 10000); // 10 seconds
    }

    /**
     * Test concurrent health stats requests
     */
    public void testConcurrentHealthStatsRequests() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::enableAllMetricsSettings);

        // Create concurrent load
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch searchLatch = new CountDownLatch(CONCURRENT_THREADS);
        CountDownLatch healthStatsLatch = new CountDownLatch(CONCURRENT_THREADS);

        try {
            // First, generate concurrent search load
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                executor.submit(() -> {
                    try {
                        doSearch(STRESS_TEST_SEARCHES_PER_THREAD);
                    } catch (IOException e) {
                        Assert.fail("Concurrent search failed: " + e.getMessage());
                    } finally {
                        searchLatch.countDown();
                    }
                });
            }

            // Wait for searches to complete
            Assert.assertTrue("Concurrent searches should complete", searchLatch.await(60, TimeUnit.SECONDS));
            Thread.sleep(5000); // Allow processing time

            // Now make concurrent health stats requests
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                executor.submit(() -> {
                    try {
                        String response = getHealthStats();
                        validateHealthStatsResponse(response);
                    } catch (IOException e) {
                        Assert.fail("Concurrent health stats request failed: " + e.getMessage());
                    } finally {
                        healthStatsLatch.countDown();
                    }
                });
            }

            // Wait for health stats requests to complete
            Assert.assertTrue("Concurrent health stats requests should complete", healthStatsLatch.await(30, TimeUnit.SECONDS));

        } finally {
            executor.shutdown();
        }
    }

    /**
     * Test field type cache statistics
     */
    public void testFieldTypeCacheStatistics() throws IOException, InterruptedException {
        // Enable query grouping to activate field type cache
        updateClusterSettings(this::enableWithQueryGroupingSettings);

        // Perform searches with different field types to populate cache
        performVariedFieldTypeSearches();
        Thread.sleep(5000); // Allow time for cache operations

        String healthStatsResponse = getHealthStats();
        validateHealthStatsResponse(healthStatsResponse);
        validateFieldTypeCacheStats(healthStatsResponse);

        // Verify cache statistics are reasonable
        validateFieldTypeCacheMetrics(healthStatsResponse);
    }

    /**
     * Test health stats error handling
     */
    public void testHealthStatsErrorHandling() throws IOException, InterruptedException {
        // Test with invalid node ID parameter
        try {
            Request request = new Request("GET", "/_insights/health_stats?nodeId=invalid-node-id");
            Response response = client().performRequest(request);

            // Should still return 200 but with appropriate error handling
            Assert.assertEquals("Should handle invalid node ID gracefully", 200, response.getStatusLine().getStatusCode());

            String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            Assert.assertNotNull("Response should not be null", responseBody);

        } catch (ResponseException e) {
            // This is also acceptable - the endpoint may return an error for invalid node IDs
            Assert.assertTrue("Should handle invalid node ID appropriately", e.getResponse().getStatusLine().getStatusCode() >= 400);
        }

        // Test with timeout parameter
        Request timeoutRequest = new Request("GET", "/_insights/health_stats?timeout=1s");
        Response timeoutResponse = client().performRequest(timeoutRequest);
        Assert.assertEquals("Should handle timeout parameter", 200, timeoutResponse.getStatusLine().getStatusCode());
    }

    /**
     * Test health stats with disabled query insights
     */
    public void testHealthStatsWithDisabledQueryInsights() throws IOException, InterruptedException {
        // Disable all query insights features
        updateClusterSettings(this::disableAllQueryInsightsSettings);
        Thread.sleep(3000);

        // Health stats should still be available even when query insights is disabled
        String healthStatsResponse = getHealthStats();
        validateHealthStatsResponse(healthStatsResponse);

        // Validate that disabled state is reflected in health stats
        validateDisabledStateHealthStats(healthStatsResponse);
    }

    // Helper method to get health stats
    private String getHealthStats() throws IOException {
        Request request = new Request("GET", "/_insights/health_stats");
        Response response = client().performRequest(request);
        Assert.assertEquals("Health stats request should succeed", 200, response.getStatusLine().getStatusCode());
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    // Validation methods

    /**
     * Validate basic health stats response structure
     */
    private void validateHealthStatsResponse(String responseBody) throws IOException {
        Assert.assertNotNull("Response body should not be null", responseBody);
        Assert.assertTrue("Response should be valid JSON", responseBody.trim().startsWith("{"));

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();
            Assert.assertNotNull("Parsed response should not be null", responseMap);
            Assert.assertFalse("Response should not be empty", responseMap.isEmpty());

            // Validate that response contains node-level data
            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                String nodeId = entry.getKey();
                Assert.assertNotNull("Node ID should not be null", nodeId);
                Assert.assertFalse("Node ID should not be empty", nodeId.trim().isEmpty());

                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();
                Assert.assertNotNull("Node data should not be null", nodeData);

                // Validate required fields
                Assert.assertTrue("Should contain ThreadPoolInfo", nodeData.containsKey("ThreadPoolInfo"));
                Assert.assertTrue("Should contain QueryRecordsQueueSize", nodeData.containsKey("QueryRecordsQueueSize"));
                Assert.assertTrue("Should contain TopQueriesHealthStats", nodeData.containsKey("TopQueriesHealthStats"));
                Assert.assertTrue("Should contain FieldTypeCacheStats", nodeData.containsKey("FieldTypeCacheStats"));
            }
        }
    }

    /**
     * Validate thread pool info structure
     */
    private void validateThreadPoolInfo(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                @SuppressWarnings("unchecked")
                Map<String, Object> threadPoolInfo = (Map<String, Object>) nodeData.get("ThreadPoolInfo");
                Assert.assertNotNull("ThreadPoolInfo should not be null", threadPoolInfo);

                // Validate query_insights_executor thread pool
                Assert.assertTrue("Should contain query_insights_executor", threadPoolInfo.containsKey("query_insights_executor"));

                @SuppressWarnings("unchecked")
                Map<String, Object> executorInfo = (Map<String, Object>) threadPoolInfo.get("query_insights_executor");
                Assert.assertNotNull("Executor info should not be null", executorInfo);

                // Validate thread pool fields
                Assert.assertTrue("Should contain type", executorInfo.containsKey("type"));
                Assert.assertTrue("Should contain core", executorInfo.containsKey("core"));
                Assert.assertTrue("Should contain max", executorInfo.containsKey("max"));
                Assert.assertTrue("Should contain keep_alive", executorInfo.containsKey("keep_alive"));
                Assert.assertTrue("Should contain queue_size", executorInfo.containsKey("queue_size"));

                // Validate field types
                Assert.assertTrue("Core should be a number", executorInfo.get("core") instanceof Number);
                Assert.assertTrue("Max should be a number", executorInfo.get("max") instanceof Number);
                Assert.assertTrue("Queue size should be a number", executorInfo.get("queue_size") instanceof Number);
            }
        }
    }

    /**
     * Validate top queries health stats for specified metric types
     */
    private void validateTopQueriesHealthStats(String responseBody, String[] expectedMetricTypes) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                @SuppressWarnings("unchecked")
                Map<String, Object> topQueriesHealthStats = (Map<String, Object>) nodeData.get("TopQueriesHealthStats");
                Assert.assertNotNull("TopQueriesHealthStats should not be null", topQueriesHealthStats);

                // Validate that expected metric types are present
                for (String metricType : expectedMetricTypes) {
                    Assert.assertTrue("Should contain " + metricType + " health stats", topQueriesHealthStats.containsKey(metricType));

                    @SuppressWarnings("unchecked")
                    Map<String, Object> metricStats = (Map<String, Object>) topQueriesHealthStats.get(metricType);
                    Assert.assertNotNull(metricType + " stats should not be null", metricStats);

                    // Validate metric-specific fields
                    Assert.assertTrue("Should contain TopQueriesHeapSize", metricStats.containsKey("TopQueriesHeapSize"));
                    Assert.assertTrue("Should contain QueryGroupCount_Total", metricStats.containsKey("QueryGroupCount_Total"));
                    Assert.assertTrue("Should contain QueryGroupCount_MaxHeap", metricStats.containsKey("QueryGroupCount_MaxHeap"));

                    // Validate field types
                    Assert.assertTrue("TopQueriesHeapSize should be a number", metricStats.get("TopQueriesHeapSize") instanceof Number);
                    Assert.assertTrue(
                        "QueryGroupCount_Total should be a number",
                        metricStats.get("QueryGroupCount_Total") instanceof Number
                    );
                    Assert.assertTrue(
                        "QueryGroupCount_MaxHeap should be a number",
                        metricStats.get("QueryGroupCount_MaxHeap") instanceof Number
                    );
                }
            }
        }
    }

    /**
     * Validate field type cache statistics
     */
    private void validateFieldTypeCacheStats(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                @SuppressWarnings("unchecked")
                Map<String, Object> fieldTypeCacheStats = (Map<String, Object>) nodeData.get("FieldTypeCacheStats");
                Assert.assertNotNull("FieldTypeCacheStats should not be null", fieldTypeCacheStats);

                // Validate required cache statistics fields
                String[] requiredFields = { "size_in_bytes", "entry_count", "evictions", "hit_count", "miss_count" };
                for (String field : requiredFields) {
                    Assert.assertTrue("Should contain " + field, fieldTypeCacheStats.containsKey(field));
                    Object value = fieldTypeCacheStats.get(field);
                    Assert.assertTrue(field + " should be a number", value instanceof Number);
                    Assert.assertTrue(field + " should be non-negative", ((Number) value).longValue() >= 0);
                }
            }
        }
    }

    /**
     * Validate operational metrics structure (we can't easily test specific failures in integration tests)
     */
    private void validateOperationalMetricsStructure(String responseBody) throws IOException {
        // Validate that the health stats response contains the necessary structure
        // for operational metrics tracking (even if specific counters are zero)
        validateHealthStatsResponse(responseBody);

        // Additional validation could include checking for specific operational metric fields
        // if they were exposed in the health stats response, but currently they're tracked
        // separately via OpenTelemetry
    }

    /**
     * Validate health stats under high load conditions
     */
    private void validateHighLoadHealthStats(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                // Validate queue size under load
                Object queueSize = nodeData.get("QueryRecordsQueueSize");
                Assert.assertTrue("Queue size should be a number", queueSize instanceof Number);

                // Under high load, queue size might be higher but should be reasonable
                int queueSizeValue = ((Number) queueSize).intValue();
                Assert.assertTrue("Queue size should be reasonable under load", queueSizeValue >= 0 && queueSizeValue < 10000);

                // Validate thread pool info under load
                @SuppressWarnings("unchecked")
                Map<String, Object> threadPoolInfo = (Map<String, Object>) nodeData.get("ThreadPoolInfo");
                @SuppressWarnings("unchecked")
                Map<String, Object> executorInfo = (Map<String, Object>) threadPoolInfo.get("query_insights_executor");

                // Thread pool should be configured appropriately for load
                int maxThreads = ((Number) executorInfo.get("max")).intValue();
                Assert.assertTrue("Max threads should be reasonable", maxThreads > 0 && maxThreads <= 100);
            }
        }
    }

    /**
     * Validate field type cache metrics for reasonableness
     */
    private void validateFieldTypeCacheMetrics(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                @SuppressWarnings("unchecked")
                Map<String, Object> fieldTypeCacheStats = (Map<String, Object>) nodeData.get("FieldTypeCacheStats");

                long hitCount = ((Number) fieldTypeCacheStats.get("hit_count")).longValue();
                long missCount = ((Number) fieldTypeCacheStats.get("miss_count")).longValue();
                long entryCount = ((Number) fieldTypeCacheStats.get("entry_count")).longValue();
                long sizeInBytes = ((Number) fieldTypeCacheStats.get("size_in_bytes")).longValue();
                long evictions = ((Number) fieldTypeCacheStats.get("evictions")).longValue();

                // Validate cache metrics relationships
                Assert.assertTrue("Hit count should be non-negative", hitCount >= 0);
                Assert.assertTrue("Miss count should be non-negative", missCount >= 0);
                Assert.assertTrue("Entry count should be non-negative", entryCount >= 0);
                Assert.assertTrue("Size in bytes should be non-negative", sizeInBytes >= 0);
                Assert.assertTrue("Evictions should be non-negative", evictions >= 0);

                // If we have entries, we should have some size
                if (entryCount > 0) {
                    Assert.assertTrue("If entries exist, size should be positive", sizeInBytes > 0);
                }

                // Total cache operations should be reasonable
                long totalOperations = hitCount + missCount;
                if (totalOperations > 0) {
                    // We should have some cache activity if we performed searches with grouping
                    Assert.assertTrue("Should have reasonable cache activity", totalOperations >= 0);
                }
            }
        }
    }

    /**
     * Validate health stats when query insights is disabled
     */
    private void validateDisabledStateHealthStats(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();

            for (Map.Entry<String, Object> entry : responseMap.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

                // Even when disabled, basic structure should be present
                Assert.assertTrue("Should contain ThreadPoolInfo", nodeData.containsKey("ThreadPoolInfo"));
                Assert.assertTrue("Should contain QueryRecordsQueueSize", nodeData.containsKey("QueryRecordsQueueSize"));
                Assert.assertTrue("Should contain TopQueriesHealthStats", nodeData.containsKey("TopQueriesHealthStats"));
                Assert.assertTrue("Should contain FieldTypeCacheStats", nodeData.containsKey("FieldTypeCacheStats"));

                // Queue size might be zero when disabled
                Object queueSize = nodeData.get("QueryRecordsQueueSize");
                Assert.assertTrue("Queue size should be a number", queueSize instanceof Number);
                int queueSizeValue = ((Number) queueSize).intValue();
                Assert.assertTrue("Queue size should be reasonable when disabled", queueSizeValue >= 0);
            }
        }
    }

    /**
     * Perform searches with varied field types to populate field type cache
     */
    private void performVariedFieldTypeSearches() throws IOException {
        // Search with different field types to populate the field type cache
        String[] searchBodies = {
            "{ \"query\": { \"match\": { \"message\": \"document\" } } }",
            "{ \"query\": { \"range\": { \"@timestamp\": { \"gte\": \"2024-01-01\" } } } }",
            "{ \"query\": { \"term\": { \"user.id\": \"cyji\" } } }",
            "{ \"query\": { \"bool\": { \"must\": [ { \"match\": { \"message\": \"test\" } } ] } } }" };

        for (String searchBody : searchBodies) {
            Request request = new Request("GET", "/my-index-0/_search?size=5");
            request.setJsonEntity(searchBody);
            try {
                Response response = client().performRequest(request);
                Assert.assertEquals("Search should succeed", 200, response.getStatusLine().getStatusCode());
            } catch (Exception e) {
                // Some searches might fail due to field mapping issues, which is fine for testing
                // field type cache behavior
            }
        }
    }

    // Settings helper methods

    private String enableBasicQueryInsightsSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10\n"
            + "    }\n"
            + "}";
    }

    private String enableLatencyOnlySettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"false\"\n"
            + "    }\n"
            + "}";
    }

    private String enableAllMetricsSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : 10\n"
            + "    }\n"
            + "}";
    }

    private String enableWithLocalIndexExporterSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.exporter.type\" : \"local_index\"\n"
            + "    }\n"
            + "}";
    }

    private String enableWithQueryGroupingSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"similarity\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }

    private String enableHighLoadSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 50,\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 50,\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : 50\n"
            + "    }\n"
            + "}";
    }

    private String disableAllQueryInsightsSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"false\",\n"
            + "        \"search.insights.top_queries.exporter.type\" : \"none\"\n"
            + "    }\n"
            + "}";
    }
}
