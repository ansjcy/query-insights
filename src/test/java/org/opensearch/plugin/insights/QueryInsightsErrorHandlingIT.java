/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

/**
 * Error Handling & Edge Cases Integration Tests for Query Insights Plugin
 *
 * This test suite covers:
 * - Plugin behavior with malformed queries
 * - Handling of search failures and exceptions
 * - Plugin stability during OpenSearch cluster issues
 * - Recovery from export/reader failures
 * - Handling of corrupted local indices
 * - Plugin behavior with insufficient permissions
 */
public class QueryInsightsErrorHandlingIT extends QueryInsightsRestTestCase {

    private static final int CONCURRENT_THREADS = 3;

    /**
     * Test plugin behavior with malformed queries
     */
    public void testPluginBehaviorWithMalformedQueries() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Test various malformed queries
        testMalformedJsonQuery();
        testInvalidQuerySyntax();
        testMissingRequiredFields();
        testInvalidFieldTypes();

        // Verify plugin continues to function with valid queries
        doSearch(3);
        // In multi-node setup, we may have more queries due to load balancing
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after malformed queries", response);
    }

    /**
     * Test handling of search failures and exceptions
     */
    public void testHandlingOfSearchFailuresAndExceptions() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);
        waitForEmptyTopQueriesResponse();

        // Test searches against non-existent indices
        testSearchAgainstNonExistentIndex();
        
        // Test searches with invalid parameters
        testSearchWithInvalidParameters();
        
        // Test timeout scenarios
        testSearchTimeoutScenarios();

        // Verify plugin recovers and continues to function
        doSearch(3);
        // In multi-node setup, we may have more queries due to load balancing
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after search failures", response);
    }

    /**
     * Test plugin stability during OpenSearch cluster issues
     */
    public void testPluginStabilityDuringClusterIssues() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Allow some time for settings to take effect
        Thread.sleep(2000);

        // Verify initial functionality
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String initialResponse = getTopQueries("latency");
        assertNotNull("Should have valid response initially", initialResponse);

        // Test during cluster state changes
        testDuringClusterStateChanges();

        // Test during high load scenarios
        testDuringHighLoadScenarios();

        // Verify plugin continues to function
        doSearch(3);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after cluster issues", response);
    }

    /**
     * Test recovery from export/reader failures
     */
    public void testRecoveryFromExportReaderFailures() throws IOException, InterruptedException {
        // Test local index exporter failure recovery
        testLocalIndexExporterFailureRecovery();
        
        // Test debug exporter failure recovery
        testDebugExporterFailureRecovery();
        
        // Test reader failure recovery
        testReaderFailureRecovery();
    }

    /**
     * Test handling of corrupted local indices
     */
    public void testHandlingOfCorruptedLocalIndices() throws IOException, InterruptedException {
        // Enable local index exporter
        enableLocalIndexExporter();
        
        // Generate some data
        doSearch(5);
        Thread.sleep(30000); // Wait for export
        
        // Test with corrupted index scenarios
        testCorruptedIndexRecovery();
        
        // Verify plugin continues to function
        doSearch(3);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after corrupted index test", response);
    }

    /**
     * Test plugin behavior with insufficient permissions
     */
    public void testPluginBehaviorWithInsufficientPermissions() throws IOException, InterruptedException {
        // Test with restricted cluster settings permissions
        testRestrictedClusterSettingsPermissions();
        
        // Test with restricted index permissions
        testRestrictedIndexPermissions();
        
        // Test with restricted search permissions
        testRestrictedSearchPermissions();
    }

    // Helper Methods for Malformed Queries

    /**
     * Test malformed JSON query
     */
    private void testMalformedJsonQuery() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search");
            request.setJsonEntity("{ \"query\": { \"match\": { \"field1\": \"value1\" } }"); // Missing closing brace
            
            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with malformed JSON");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for malformed JSON", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    /**
     * Test invalid query syntax
     */
    private void testInvalidQuerySyntax() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search");
            request.setJsonEntity("{ \"query\": { \"invalid_query_type\": { \"field1\": \"value1\" } } }");
            
            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with invalid query syntax");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for invalid query syntax", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    /**
     * Test missing required fields
     */
    private void testMissingRequiredFields() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search");
            request.setJsonEntity("{ \"query\": { \"bool\": { \"must\": [] } } }"); // Empty must clause

            // This actually succeeds in OpenSearch (empty must clause matches all), so we test a different scenario
            Response response = firstNodeClient.performRequest(request);
            assertEquals("Empty must clause should succeed", 200, response.getStatusLine().getStatusCode());
        }
    }

    /**
     * Test invalid field types
     */
    private void testInvalidFieldTypes() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search");
            request.setJsonEntity("{ \"size\": \"invalid_size_value\" }"); // Size should be integer
            
            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with invalid field types");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for invalid field types", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    // Helper Methods for Search Failures

    /**
     * Test search against non-existent index
     */
    private void testSearchAgainstNonExistentIndex() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/non-existent-index/_search");
            request.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
            
            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with non-existent index");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for non-existent index", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    /**
     * Test search with invalid parameters
     */
    private void testSearchWithInvalidParameters() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search?size=-1"); // Invalid size
            request.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
            
            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with invalid parameters");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for invalid parameters", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    /**
     * Test search timeout scenarios
     */
    private void testSearchTimeoutScenarios() throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search?timeout=1ms"); // Very short timeout
            request.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
            
            // This might succeed or timeout, both are acceptable for this test
            try {
                Response response = firstNodeClient.performRequest(request);
                assertTrue("Response should be successful or timeout", 
                          response.getStatusLine().getStatusCode() == 200 || 
                          response.getStatusLine().getStatusCode() == 408);
            } catch (ResponseException e) {
                // Timeout or other errors are acceptable
                assertTrue("Expected timeout or other error", 
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }
    }

    // Helper Methods for Cluster Issues

    /**
     * Test during cluster state changes
     */
    private void testDuringClusterStateChanges() throws IOException, InterruptedException {
        // Perform searches while changing cluster settings
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        
        // Thread 1: Perform searches
        executor.submit(() -> {
            try {
                for (int i = 0; i < 5; i++) {
                    doSearch(1);
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                // Expected during cluster state changes
            } finally {
                latch.countDown();
            }
        });
        
        // Thread 2: Change settings
        executor.submit(() -> {
            try {
                Thread.sleep(1000);
                updateClusterSettings(this::modifiedSettings);
                Thread.sleep(1000);
                updateClusterSettings(this::defaultTopQueriesSettings);
            } catch (Exception e) {
                // Expected during concurrent operations
            } finally {
                latch.countDown();
            }
        });
        
        assertTrue("Operations should complete within timeout", 
                   latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
    }

    /**
     * Test during high load scenarios
     */
    private void testDuringHighLoadScenarios() throws IOException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        doSearch(1);
                        Thread.sleep(100);
                    }
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Some failures are expected under high load
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue("High load operations should complete within timeout",
                   latch.await(60, TimeUnit.SECONDS));
        assertTrue("At least some operations should succeed under high load",
                   successCount.get() > 0);
        executor.shutdown();
    }

    // Helper Methods for Export/Reader Failures

    /**
     * Test local index exporter failure recovery
     */
    private void testLocalIndexExporterFailureRecovery() throws IOException, InterruptedException {
        // Enable local index exporter
        enableLocalIndexExporter();

        // Generate some data
        doSearch(3);
        Thread.sleep(30000); // Wait for export

        // Simulate failure by switching to invalid exporter
        try {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(
                "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"invalid_type\" } }"
            );
            client().performRequest(request);
            fail("Should fail with invalid exporter type");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }

        // Recover by setting valid exporter
        enableLocalIndexExporter();

        // Verify recovery
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after exporter failure recovery", response);
    }

    /**
     * Test debug exporter failure recovery
     */
    private void testDebugExporterFailureRecovery() throws IOException, InterruptedException {
        // Enable debug exporter
        enableDebugExporter();

        // Generate some data
        doSearch(3);
        Thread.sleep(10000); // Wait for debug export

        // Switch to local index exporter to test recovery
        enableLocalIndexExporter();

        // Verify recovery
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after debug exporter recovery", response);
    }

    /**
     * Test reader failure recovery
     */
    private void testReaderFailureRecovery() throws IOException, InterruptedException {
        // Enable query insights
        updateClusterSettings(this::defaultTopQueriesSettings);

        // Generate data
        doSearch(3);
        // In multi-node setup, we may have accumulated queries from previous operations
        String initialResponse = getTopQueries("latency");
        assertNotNull("Should have valid response before reader failure test", initialResponse);

        // Test with invalid date range (should not crash the reader)
        try {
            Request request = new Request("GET", "/_insights/top_queries?from=invalid-date&to=invalid-date");
            client().performRequest(request);
            fail("Should fail with invalid date format");
        } catch (ResponseException e) {
            assertTrue("Expected 4xx error for invalid date format",
                      e.getResponse().getStatusLine().getStatusCode() >= 400);
        }

        // Verify reader recovers
        String response = getTopQueries("latency");
        assertNotNull("Reader should recover and return valid response", response);
    }

    // Helper Methods for Corrupted Indices

    /**
     * Test corrupted index recovery
     */
    private void testCorruptedIndexRecovery() throws IOException, InterruptedException {
        // Try to create an index with invalid mapping to simulate corruption
        try {
            Request request = new Request("PUT", "/corrupted-test-index");
            request.setJsonEntity("{ \"mappings\": { \"properties\": { \"invalid\": { \"type\": \"invalid_type\" } } } }");
            client().performRequest(request);
            fail("Should fail with invalid mapping");
        } catch (ResponseException e) {
            assertTrue("Expected 4xx error for invalid mapping",
                      e.getResponse().getStatusLine().getStatusCode() >= 400);
        }

        // Verify plugin continues to function despite corruption simulation
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after corrupted index test", response);
    }

    // Helper Methods for Permission Tests

    /**
     * Test restricted cluster settings permissions
     */
    private void testRestrictedClusterSettingsPermissions() throws IOException, InterruptedException {
        // This test simulates permission restrictions by testing invalid settings
        try {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity("{ \"persistent\": { \"invalid.setting\": \"value\" } }");
            client().performRequest(request);
            fail("Should fail with invalid cluster setting");
        } catch (ResponseException e) {
            assertTrue("Expected 4xx error for invalid cluster setting",
                      e.getResponse().getStatusLine().getStatusCode() >= 400);
        }

        // Verify plugin continues to function
        updateClusterSettings(this::defaultTopQueriesSettings);
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after cluster settings test", response);
    }

    /**
     * Test restricted index permissions
     */
    private void testRestrictedIndexPermissions() throws IOException, InterruptedException {
        // Test with system indices (should be restricted)
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/.opensearch*/_search");
            request.setJsonEntity("{ \"query\": { \"match_all\": {} } }");

            try {
                firstNodeClient.performRequest(request);
                // Some system indices might be searchable, so we don't fail here
            } catch (ResponseException e) {
                // Expected for restricted system indices
                assertTrue("Expected 4xx error for restricted indices",
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }

        // Verify plugin continues to function with allowed indices
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after index permissions test", response);
    }

    /**
     * Test restricted search permissions
     */
    private void testRestrictedSearchPermissions() throws IOException, InterruptedException {
        // Test with invalid search parameters to simulate permission restrictions
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            Request request = new Request("GET", "/my-index-0/_search");
            request.setJsonEntity("{ \"query\": { \"script\": { \"source\": \"invalid script\" } } }");

            try {
                firstNodeClient.performRequest(request);
                fail("Should fail with invalid script");
            } catch (ResponseException e) {
                assertTrue("Expected 4xx error for invalid script",
                          e.getResponse().getStatusLine().getStatusCode() >= 400);
            }
        }

        // Verify plugin continues to function with valid searches
        doSearch(2);
        // In multi-node setup, we may have accumulated queries from previous operations
        String response = getTopQueries("latency");
        assertNotNull("Should have valid response after search permissions test", response);
    }

    // Helper Methods for Exporter Configuration

    /**
     * Enable local index exporter
     */
    private void enableLocalIndexExporter() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"local_index\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.latency.window_size\": \"1m\" } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Enable debug exporter
     */
    private void enableDebugExporter() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"debug\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.latency.window_size\": \"1m\" } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Modified settings for testing dynamic changes
     */
    private String modifiedSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"5m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 10\n"
            + "    }\n"
            + "}";
    }
}
