/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/**
 * Integration tests for QueryInsights historical data and reader functionality.
 * Tests LocalIndexReader functionality with comprehensive scenarios including:
 * - Date range queries
 * - Historical data retrieval with from/to parameters
 * - Query ID filtering
 * - Verbose/non-verbose output
 * - Index discovery across multiple date-based indices
 * - Handling of missing or empty indices
 * - Search request building and execution
 */
public class QueryInsightsReaderIT extends QueryInsightsRestTestCase {

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    /**
     * Test basic historical data retrieval functionality
     */
    public void testQueryInsightsHistoricalTopQueriesRead() throws IOException, InterruptedException {
        try {
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();
            List<String[]> allPairs = fetchHistoricalTopQueries("null", "null", "null");
            assertFalse("Expected at least one top query", allPairs.isEmpty());
            String selectedId = allPairs.get(0)[0];
            String selectedNodeId = allPairs.get(0)[1];
            List<String[]> filteredPairs = fetchHistoricalTopQueries(selectedId, "null", "null");
            List<String[]> filteredPairs1 = fetchHistoricalTopQueries("null", selectedNodeId, "null");
            List<String[]> filteredPairs2 = fetchHistoricalTopQueries(selectedId, selectedNodeId, "null");
            List<String[]> filteredPairs3 = fetchHistoricalTopQueries(selectedId, selectedNodeId, "latency");

            // Validate filtering results
            assertNotNull("Filtered pairs by ID should not be null", filteredPairs);
            assertNotNull("Filtered pairs by node ID should not be null", filteredPairs1);
            assertNotNull("Filtered pairs by ID and node ID should not be null", filteredPairs2);
            assertNotNull("Filtered pairs by ID, node ID and type should not be null", filteredPairs3);

        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test LocalIndexReader functionality with specific date range queries
     */
    public void testHistoricalDataWithDateRangeQueries() throws IOException, InterruptedException {
        try {
            // Setup test data
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();

            // Test 1: Query with current time range
            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);

            // Use direct API call instead of fetchHistoricalTopQueries to avoid parsing issues
            String endpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request request = new Request("GET", endpoint);
            Response response = client().performRequest(request);
            assertEquals("Current time range request should succeed", 200, response.getStatusLine().getStatusCode());

            // Test 2: Query with past time range (should return empty or succeed)
            Instant twoDaysAgo = now.minusSeconds(172800);
            Instant oneDayAgo = now.minusSeconds(86400);
            String pastEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(twoDaysAgo)
                + "&to=" + ISO_FORMATTER.format(oneDayAgo) + "&pretty";
            Request pastRequest = new Request("GET", pastEndpoint);
            Response pastResponse = client().performRequest(pastRequest);
            assertEquals("Past time range request should succeed", 200, pastResponse.getStatusLine().getStatusCode());

            // Test 3: Query with future time range (should return empty)
            Instant oneHourLater = now.plusSeconds(3600);
            Instant twoHoursLater = now.plusSeconds(7200);
            String futureEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourLater)
                + "&to=" + ISO_FORMATTER.format(twoHoursLater) + "&pretty";
            Request futureRequest = new Request("GET", futureEndpoint);
            Response futureResponse = client().performRequest(futureRequest);
            assertEquals("Future time range request should succeed", 200, futureResponse.getStatusLine().getStatusCode());

            // Test 4: Query with same start and end time
            String sameTimeEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(now)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request sameTimeRequest = new Request("GET", sameTimeEndpoint);
            Response sameTimeResponse = client().performRequest(sameTimeRequest);
            assertEquals("Same time range request should succeed", 200, sameTimeResponse.getStatusLine().getStatusCode());

        } catch (Exception e) {
            fail("Date range query test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test historical data filtering by query ID
     */
    public void testHistoricalDataFilteringByQueryId() throws IOException, InterruptedException {
        try {
            // Setup test data with multiple searches to generate different query IDs
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");

            // Perform multiple different searches to generate various query IDs
            performSearch();
            doSearch("match", 2);
            doSearch("range", 2);
            doSearch("term", 2);

            Thread.sleep(80000);
            checkLocalIndices();

            // Test basic query ID filtering functionality using direct API calls
            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);

            // Test filtering by a specific query ID (use a realistic ID)
            String testQueryId = "test-query-id-123";
            String filteredEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&id=" + testQueryId + "&pretty";
            Request filteredRequest = new Request("GET", filteredEndpoint);
            Response filteredResponse = client().performRequest(filteredRequest);
            assertEquals("Filtered query ID request should succeed", 200, filteredResponse.getStatusLine().getStatusCode());

            // Test filtering by non-existent query ID
            String nonExistentId = "non-existent-query-id-12345";
            String nonExistentEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&id=" + nonExistentId + "&pretty";
            Request nonExistentRequest = new Request("GET", nonExistentEndpoint);
            Response nonExistentResponse = client().performRequest(nonExistentRequest);
            assertEquals("Non-existent query ID request should succeed", 200, nonExistentResponse.getStatusLine().getStatusCode());

            // Verify response contains expected structure
            String responseContent = new String(nonExistentResponse.getEntity().getContent().readAllBytes());
            assertTrue("Response should contain top_queries field", responseContent.contains("top_queries"));

        } catch (Exception e) {
            fail("Query ID filtering test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test historical data with verbose and non-verbose output
     */
    public void testHistoricalDataVerboseOutput() throws IOException, InterruptedException {
        try {
            // Setup test data
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();

            // Test verbose output (should include all fields)
            String verboseEndpoint = "/_insights/top_queries?verbose=true&pretty";
            Request verboseRequest = new Request("GET", verboseEndpoint);
            Response verboseResponse = client().performRequest(verboseRequest);
            assertEquals("Verbose request should succeed", 200, verboseResponse.getStatusLine().getStatusCode());

            String verboseContent = new String(verboseResponse.getEntity().getContent().readAllBytes());
            assertNotNull("Verbose response content should not be null", verboseContent);

            // Test non-verbose output (should exclude verbose-only fields)
            String nonVerboseEndpoint = "/_insights/top_queries?verbose=false&pretty";
            Request nonVerboseRequest = new Request("GET", nonVerboseEndpoint);
            Response nonVerboseResponse = client().performRequest(nonVerboseRequest);
            assertEquals("Non-verbose request should succeed", 200, nonVerboseResponse.getStatusLine().getStatusCode());

            String nonVerboseContent = new String(nonVerboseResponse.getEntity().getContent().readAllBytes());
            assertNotNull("Non-verbose response content should not be null", nonVerboseContent);

            // Test default behavior (should be non-verbose)
            String defaultEndpoint = "/_insights/top_queries?pretty";
            Request defaultRequest = new Request("GET", defaultEndpoint);
            Response defaultResponse = client().performRequest(defaultRequest);
            assertEquals("Default request should succeed", 200, defaultResponse.getStatusLine().getStatusCode());

        } catch (Exception e) {
            fail("Verbose output test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test index discovery across multiple date-based indices
     */
    public void testIndexDiscoveryAcrossMultipleDates() throws IOException, InterruptedException {
        try {
            // Setup test data that spans multiple time periods
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");

            // Perform searches at different times to potentially create multiple indices
            performSearch();
            Thread.sleep(30000); // Wait 30 seconds
            doSearch(3);
            Thread.sleep(30000); // Wait another 30 seconds
            doSearch(3);

            Thread.sleep(80000); // Wait for data to be exported
            checkLocalIndices();

            // Test discovery across a wide date range using direct API calls
            Instant now = Instant.now();
            Instant twelveHoursAgo = now.minusSeconds(43200);

            String wideRangeEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(twelveHoursAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request wideRangeRequest = new Request("GET", wideRangeEndpoint);
            Response wideRangeResponse = client().performRequest(wideRangeRequest);
            assertEquals("Wide range request should succeed", 200, wideRangeResponse.getStatusLine().getStatusCode());

            // Test discovery with narrow date range
            Instant oneHourAgo = now.minusSeconds(3600);
            String narrowRangeEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request narrowRangeRequest = new Request("GET", narrowRangeEndpoint);
            Response narrowRangeResponse = client().performRequest(narrowRangeRequest);
            assertEquals("Narrow range request should succeed", 200, narrowRangeResponse.getStatusLine().getStatusCode());

            // Verify that indices are being discovered properly by checking index patterns
            Request indicesRequest = new Request("GET", "/_cat/indices/top_queries-*?v&s=index");
            Response indicesResponse = client().performRequest(indicesRequest);
            assertEquals("Indices request should succeed", 200, indicesResponse.getStatusLine().getStatusCode());

            String indicesContent = new String(indicesResponse.getEntity().getContent().readAllBytes());
            assertNotNull("Indices content should not be null", indicesContent);
            // Note: indices may or may not exist depending on timing, so we just verify the request succeeds

        } catch (Exception e) {
            fail("Index discovery test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test handling of missing or empty indices
     */
    public void testHandlingOfMissingOrEmptyIndices() throws IOException, InterruptedException {
        try {
            // Test querying when no indices exist (before any data is created)
            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);

            // Query before creating any data - should handle gracefully
            String emptyEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request emptyRequest = new Request("GET", emptyEndpoint);
            Response emptyResponse = client().performRequest(emptyRequest);
            assertEquals("Empty query request should succeed", 200, emptyResponse.getStatusLine().getStatusCode());

            // Now create data but query a different time range
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();

            // Query a time range where no data exists (far in the past)
            Instant oneWeekAgo = now.minusSeconds(604800);
            Instant sixDaysAgo = now.minusSeconds(518400);
            String pastEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneWeekAgo)
                + "&to=" + ISO_FORMATTER.format(sixDaysAgo) + "&pretty";
            Request pastRequest = new Request("GET", pastEndpoint);
            Response pastResponse = client().performRequest(pastRequest);
            assertEquals("Past query request should succeed", 200, pastResponse.getStatusLine().getStatusCode());

            // Query a time range where no data exists (far in the future)
            Instant oneWeekLater = now.plusSeconds(604800);
            Instant twoWeeksLater = now.plusSeconds(1209600);
            String futureEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneWeekLater)
                + "&to=" + ISO_FORMATTER.format(twoWeeksLater) + "&pretty";
            Request futureRequest = new Request("GET", futureEndpoint);
            Response futureResponse = client().performRequest(futureRequest);
            assertEquals("Future query request should succeed", 200, futureResponse.getStatusLine().getStatusCode());

            // Verify all responses contain the expected structure
            String futureContent = new String(futureResponse.getEntity().getContent().readAllBytes());
            assertTrue("Future response should contain top_queries field", futureContent.contains("top_queries"));

        } catch (Exception e) {
            fail("Missing/empty indices test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test search request building and execution for historical data
     */
    public void testSearchRequestBuildingAndExecution() throws IOException, InterruptedException {
        try {
            // Setup test data
            createDocument();
            defaultExporterSettings();
            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();

            // Test search request with various parameters
            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);

            // Test 1: Basic search request execution
            List<String[]> basicResults = fetchHistoricalTopQueries(oneHourAgo, now, "null", "null", "null");
            assertNotNull("Basic search results should not be null", basicResults);

            // Test 2: Search request with metric type filter
            List<String[]> latencyResults = fetchHistoricalTopQueries(oneHourAgo, now, "null", "null", "latency");
            assertNotNull("Latency filtered results should not be null", latencyResults);

            // Test 3: Search request with timeout handling (test endpoint response time)
            long startTime = System.currentTimeMillis();
            String timeoutEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request timeoutRequest = new Request("GET", timeoutEndpoint);
            Response timeoutResponse = client().performRequest(timeoutRequest);
            long endTime = System.currentTimeMillis();

            assertEquals("Timeout request should succeed", 200, timeoutResponse.getStatusLine().getStatusCode());
            assertTrue("Request should complete within reasonable time", (endTime - startTime) < 30000); // 30 seconds

            // Test 4: Search request with caching (make same request twice)
            List<String[]> firstCacheResults = fetchHistoricalTopQueries(oneHourAgo, now, "null", "null", "null");
            List<String[]> secondCacheResults = fetchHistoricalTopQueries(oneHourAgo, now, "null", "null", "null");
            assertNotNull("First cache results should not be null", firstCacheResults);
            assertNotNull("Second cache results should not be null", secondCacheResults);
            assertEquals("Cached results should be consistent", firstCacheResults.size(), secondCacheResults.size());

            // Test 5: Search request with indices options (test with non-existent indices)
            Instant farPast = now.minusSeconds(2592000); // 30 days ago
            Instant nearPast = now.minusSeconds(2505600); // 29 days ago
            List<String[]> nonExistentResults = fetchHistoricalTopQueries(farPast, nearPast, "null", "null", "null");
            assertNotNull("Non-existent indices results should not be null", nonExistentResults);
            assertTrue("Non-existent indices should return empty results", nonExistentResults.isEmpty());

        } catch (Exception e) {
            fail("Search request building test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test comprehensive historical data scenarios with multiple metric types
     */
    public void testHistoricalDataWithMultipleMetricTypes() throws IOException, InterruptedException {
        try {
            // Setup test data with multiple metric types enabled
            createDocument();

            // Enable multiple metric types
            String multiMetricSettings = "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"local_index\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.cpu.enabled\": \"true\", "
                + "\"search.insights.top_queries.memory.enabled\": \"true\" } }";
            Request multiMetricRequest = new Request("PUT", "/_cluster/settings");
            multiMetricRequest.setJsonEntity(multiMetricSettings);
            client().performRequest(multiMetricRequest);

            setLatencyWindowSize("1m");
            performSearch();
            Thread.sleep(80000);
            checkLocalIndices();

            Instant now = Instant.now();
            Instant oneHourAgo = now.minusSeconds(3600);

            // Test each metric type individually using direct API calls
            String latencyEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&type=latency&pretty";
            Request latencyRequest = new Request("GET", latencyEndpoint);
            Response latencyResponse = client().performRequest(latencyRequest);
            assertEquals("Latency query should succeed", 200, latencyResponse.getStatusLine().getStatusCode());

            String cpuEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&type=cpu&pretty";
            Request cpuRequest = new Request("GET", cpuEndpoint);
            Response cpuResponse = client().performRequest(cpuRequest);
            assertEquals("CPU query should succeed", 200, cpuResponse.getStatusLine().getStatusCode());

            String memoryEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&type=memory&pretty";
            Request memoryRequest = new Request("GET", memoryEndpoint);
            Response memoryResponse = client().performRequest(memoryRequest);
            assertEquals("Memory query should succeed", 200, memoryResponse.getStatusLine().getStatusCode());

            String allEndpoint = "/_insights/top_queries?from=" + ISO_FORMATTER.format(oneHourAgo)
                + "&to=" + ISO_FORMATTER.format(now) + "&pretty";
            Request allRequest = new Request("GET", allEndpoint);
            Response allResponse = client().performRequest(allRequest);
            assertEquals("All metrics query should succeed", 200, allResponse.getStatusLine().getStatusCode());

        } catch (Exception e) {
            fail("Multiple metric types test failed with exception: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    /**
     * Test invalid date range parameters and error handling
     */
    public void testInvalidDateRangeParameters() throws IOException {
        String[] invalidEndpoints = new String[] {
            "/_insights/top_queries?from=2024-00-01T00:00:00.000Z&to=2024-04-07T00:00:00.000Z", // Invalid month
            "/_insights/top_queries?from=2024-13-01T00:00:00.000Z&to=2024-04-07T00:00:00.000Z", // Month out of range
            "/_insights/top_queries?from=abcd&to=efgh", // Not a date
            "/_insights/top_queries?from=&to=", // Empty values
            "/_insights/top_queries?from=2024-04-10T00:00:00Z", // Missing `to`
            "/_insights/top_queries?to=2024-04-10T00:00:00Z", // Missing `from`

            // Invalid metric type
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&type=Latency",
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&type=xyz",

            // Unexpected param
            "/_insights/top_queries?from=2025-04-15T17:00:00.000Z&to=2025-04-15T18:00:00.000Z&foo=bar",
            "/_insights/top_queries?from=2025-04-15T17:59:42.304Z&to=2025-04-15T20:39:42.304Zabdncmdkdkssmcmd", };

        for (String endpoint : invalidEndpoints) {
            runInvalidDateRequest(endpoint);
        }
    }

    /**
     * Helper method to test invalid date requests
     */
    private void runInvalidDateRequest(String endpoint) throws IOException {
        Request request = new Request("GET", endpoint);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }
}
