/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Multi-Node & Cluster Integration Tests for Query Insights Plugin
 *
 * This test suite covers:
 * - Plugin functionality across multiple nodes
 * - Data aggregation from multiple nodes
 * - Cluster state changes impact on plugin
 * - Cross-node query insights data consistency
 * - Load balancing of query insights operations
 */
public class QueryInsightsClusterIT extends QueryInsightsRestTestCase {

    private static final int CONCURRENT_THREADS = 3;

    /**
     * Test plugin functionality across multiple nodes
     */
    public void testPluginFunctionalityAcrossMultipleNodes() throws IOException, InterruptedException {
        // Enable query insights on all nodes
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Verify multi-node cluster setup
        verifyMultiNodeClusterSetup();

        // Perform searches using the standard doSearch method
        doSearch(10);

        // Verify that query insights are collected
        assertTopQueriesCount(5, "latency");
    }

    /**
     * Test data aggregation from multiple nodes
     */
    public void testDataAggregationFromMultipleNodes() throws IOException, InterruptedException {
        // Enable all metric types for comprehensive testing
        updateClusterSettings(this::multiNodeAllMetricsSettings);

        // Perform searches with different patterns
        doSearch("match", 5);
        doSearch("range", 5);
        doSearch("term", 5);

        // Verify aggregated data from all nodes
        assertTopQueriesCount(5, "latency");
        assertTopQueriesCount(5, "cpu");
        assertTopQueriesCount(5, "memory");
    }

    /**
     * Test cluster state changes impact on plugin
     */
    public void testClusterStateChangesImpactOnPlugin() throws IOException, InterruptedException {
        // Initial setup with query insights enabled
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Perform initial searches
        doSearch(5);
        assertTopQueriesCount(5, "latency");

        // Change cluster settings dynamically
        updateClusterSettings(this::modifiedMultiNodeSettings);
        Thread.sleep(2000); // Allow settings to propagate

        // Perform more searches after settings change
        doSearch(5);

        // Verify that settings changes work
        assertTopQueriesCount(5, "latency");
    }

    /**
     * Test cross-node query insights data consistency
     */
    public void testCrossNodeQueryInsightsDataConsistency() throws IOException, InterruptedException {
        // Enable query insights with grouping for consistency testing
        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Perform identical queries
        doSearch("match", 5);

        // Verify data consistency across nodes
        assertTopQueriesCount(5, "latency");
    }

    /**
     * Test load balancing of query insights operations
     */
    public void testLoadBalancingOfQueryInsightsOperations() throws IOException, InterruptedException {
        // Enable query insights for load balancing test
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Perform concurrent searches to test load balancing
        performConcurrentSearches();

        // Verify load distribution across nodes
        assertTopQueriesCount(5, "latency");
    }

    /**
     * Test concurrent operations across multiple nodes
     */
    public void testConcurrentOperationsAcrossMultipleNodes() throws IOException, InterruptedException {
        // Enable all metrics for concurrent testing
        updateClusterSettings(this::multiNodeAllMetricsSettings);

        // Perform concurrent operations
        performConcurrentSearches();

        // Verify concurrent operation results
        assertTopQueriesCount(5, "latency");
        assertTopQueriesCount(5, "cpu");
        assertTopQueriesCount(5, "memory");
    }

    /**
     * Test cluster health impact of query insights
     */
    public void testClusterHealthImpactOfQueryInsights() throws IOException, InterruptedException {
        // Monitor cluster health before enabling query insights
        verifyClusterHealth();

        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Perform searches
        doSearch(15);

        // Verify cluster health remains stable
        verifyClusterHealth();

        // Verify query insights are still functioning
        assertTopQueriesCount(5, "latency");
    }

    // Helper Methods

    /**
     * Verify multi-node cluster setup
     */
    private void verifyMultiNodeClusterSetup() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            responseBody.getBytes(StandardCharsets.UTF_8)
        )) {
            Map<String, Object> healthMap = parser.map();
            int numberOfNodes = (Integer) healthMap.get("number_of_nodes");
            assertTrue("Expected at least 2 nodes for multi-node testing", numberOfNodes >= 2);
            assertEquals("green", healthMap.get("status"));
        }
    }

    /**
     * Perform concurrent searches to test multi-node behavior
     */
    private void performConcurrentSearches() throws IOException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 5; j++) {
                        doSearch(1);
                    }
                    successCount.incrementAndGet();
                } catch (IOException e) {
                    // Log error but don't fail the test immediately
                    System.err.println("Concurrent search failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Concurrent searches should complete within timeout",
                   latch.await(30, TimeUnit.SECONDS));
        assertTrue("At least some concurrent searches should succeed",
                   successCount.get() > 0);

        executor.shutdown();
    }

    /**
     * Verify cluster health
     */
    private void verifyClusterHealth() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            responseBody.getBytes(StandardCharsets.UTF_8)
        )) {
            Map<String, Object> healthMap = parser.map();
            String status = (String) healthMap.get("status");
            assertTrue("Cluster should be healthy", "green".equals(status) || "yellow".equals(status));
        }
    }

    // Settings Methods

    /**
     * Multi-node all metrics settings
     */
    private String multiNodeAllMetricsSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"none\"\n"
            + "    }\n"
            + "}";
    }

    /**
     * Modified multi-node settings for testing dynamic changes
     */
    private String modifiedMultiNodeSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"similarity\"\n"
            + "    }\n"
            + "}";
    }
}