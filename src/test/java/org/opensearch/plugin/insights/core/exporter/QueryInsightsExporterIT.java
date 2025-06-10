/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Integration tests for Query Insights data export functionality
 */
public class QueryInsightsExporterIT extends QueryInsightsRestTestCase {

    /**
     * Test comprehensive exporter settings validation and functionality
     */
    public void testQueryInsightsExporterSettings() throws Exception {
        createDocument();
        for (String setting : invalidExporterSettings()) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(setting);
            try {
                client().performRequest(request);
                fail("Should not succeed with invalid exporter settings");
            } catch (ResponseException e) {
                assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            }
        }
        defaultExporterSettings();// Enabling Local index Setting
        setLatencyWindowSize("1m");

        // Perform multiple searches to ensure query insights data is collected
        for (int i = 0; i < 5; i++) {
            performSearch();
            Thread.sleep(2000); // Small delay between searches
        }

        Thread.sleep(70000); // Allow time for export to local index
        checkLocalIndices();
        checkQueryInsightsIndexTemplate();
        cleanupIndextemplate();
        disableLocalIndexExporter();
        defaultExporterSettings();// Re-enabling the Local Index
        setLocalIndexToDebug();// Ensuring it is able to toggle Local to Debug
        cleanup();
    }

    /**
     * Test LOCAL_INDEX exporter functionality
     */
    public void testLocalIndexExporterFunctionality() throws Exception {
        try {
            // Setup: Enable local index exporter
            enableLocalIndexExporter();
            performMultipleSearches(5);

            // Wait for export to complete
            Thread.sleep(70000);

            // Verify index creation and data export
            verifyLocalIndexCreation();
            verifyExportedDataStructure();
            verifyIndexMappings();

        } finally {
            cleanup();
        }
    }

    /**
     * Test DEBUG exporter functionality
     */
    public void testDebugExporterFunctionality() throws Exception {
        try {
            // Setup: Enable debug exporter
            enableDebugExporter();
            performMultipleSearches(3);

            // Wait for debug export to complete
            Thread.sleep(30000);

            // Verify debug exporter is active (no indices should be created)
            verifyNoLocalIndicesCreated();

        } finally {
            cleanup();
        }
    }

    /**
     * Test switching between different exporter types
     */
    public void testExporterTypeSwitching() throws Exception {
        try {
            // Ensure document exists for all searches
            createDocument();

            // Test LOCAL_INDEX -> DEBUG switching
            enableLocalIndexExporter();
            performSearch();
            Thread.sleep(30000);

            // Switch to DEBUG
            enableDebugExporter();
            performSearch();
            Thread.sleep(30000);

            // Switch to NONE
            disableAllExporters();
            performSearch();
            Thread.sleep(30000);

            // Switch back to LOCAL_INDEX
            enableLocalIndexExporter();
            performSearch();
            Thread.sleep(30000);

            // Verify final state
            verifyLocalIndexCreation();

        } finally {
            cleanup();
        }
    }

    /**
     * Test local index creation with proper mappings and templates
     */
    public void testLocalIndexCreationAndTemplates() throws Exception {
        try {
            // Ensure document exists for search
            createDocument();

            // Enable local index exporter
            enableLocalIndexExporter();
            performSearch();
            Thread.sleep(70000);

            // Verify index template creation
            verifyIndexTemplateCreation();
            verifyIndexTemplatePriority();
            verifyIndexMappings();

        } finally {
            cleanup();
        }
    }

    /**
     * Test bulk export operations
     */
    public void testBulkExportOperations() throws Exception {
        try {
            // Enable local index exporter
            enableLocalIndexExporter();

            // Perform multiple searches to generate bulk data
            performMultipleSearches(10);
            Thread.sleep(70000);

            // Verify bulk export succeeded
            verifyBulkExportData();
            verifyMultipleRecordsExported();

        } finally {
            cleanup();
        }
    }

    /**
     * Test export failure handling and retry mechanisms
     */
    public void testExportFailureHandling() throws Exception {
        try {
            // Test with invalid exporter configuration
            testInvalidExporterConfiguration();

            // Ensure document exists for search
            createDocument();

            // Test recovery after configuration fix
            enableLocalIndexExporter();
            performSearch();
            Thread.sleep(30000);

            // Verify recovery
            verifyLocalIndexCreation();

        } finally {
            cleanup();
        }
    }

    /**
     * Test index template priority settings
     */
    public void testIndexTemplatePrioritySettings() throws Exception {
        try {
            // Ensure document exists for search
            createDocument();

            // Enable local index exporter with custom template priority
            enableLocalIndexExporterWithCustomPriority(2000L);
            performSearch();
            Thread.sleep(70000);

            // Verify template priority
            verifyCustomTemplatePriority(2000L);

        } finally {
            cleanup();
        }
    }

    /**
     * Test concurrent export operations
     */
    public void testConcurrentExportOperations() throws Exception {
        try {
            // Enable local index exporter
            enableLocalIndexExporter();

            // Perform concurrent searches (createDocument is called inside performConcurrentSearches)
            performConcurrentSearches();
            Thread.sleep(70000);

            // Verify all exports succeeded
            verifyConcurrentExportResults();

        } finally {
            cleanup();
        }
    }

    // Helper Methods

    /**
     * Enable local index exporter
     */
    protected void enableLocalIndexExporter() throws IOException {
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
    protected void enableDebugExporter() throws IOException {
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
     * Disable all exporters
     */
    protected void disableAllExporters() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"none\", "
                + "\"search.insights.top_queries.latency.enabled\": \"false\" } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Enable local index exporter with custom template priority
     */
    protected void enableLocalIndexExporterWithCustomPriority(long priority) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { "
                + "\"search.insights.top_queries.exporter.type\": \"local_index\", "
                + "\"search.insights.top_queries.latency.enabled\": \"true\", "
                + "\"search.insights.top_queries.latency.window_size\": \"1m\", "
                + "\"search.insights.top_queries.exporter.template_priority\": " + priority + " } }"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Perform multiple searches to generate data for export
     */
    protected void performMultipleSearches(int count) throws IOException, InterruptedException {
        // Ensure document exists before performing searches
        createDocument();

        for (int i = 0; i < count; i++) {
            performSearch();
            // Small delay between searches to avoid overwhelming
            Thread.sleep(100);
        }
    }

    /**
     * Perform concurrent searches
     */
    protected void performConcurrentSearches() throws IOException {
        // Ensure document exists before performing searches
        createDocument();

        // Perform searches with different queries to generate diverse data
        String[] queries = {
            "{ \"query\": { \"match_all\": {} } }",
            "{ \"query\": { \"term\": { \"user.id\": \"cyji\" } } }",
            "{ \"query\": { \"range\": { \"@timestamp\": { \"gte\": \"2024-04-01\" } } } }",
            "{ \"query\": { \"match\": { \"message\": \"document\" } } }",
            "{ \"query\": { \"match\": { \"title\": \"Test Document\" } } }"
        };

        for (String query : queries) {
            Request searchRequest = new Request("POST", "/my-index-0/_search");
            searchRequest.setJsonEntity(query);
            client().performRequest(searchRequest);
        }
    }

    /**
     * Verify local index creation
     */
    protected void verifyLocalIndexCreation() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected top_queries-* index to be created", responseContent.contains("top_queries-"));
        assertTrue("Expected top_queries-* index to be green", responseContent.contains("green"));
    }

    /**
     * Verify no local indices were created (for debug exporter test)
     */
    protected void verifyNoLocalIndicesCreated() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertFalse("No top_queries-* indices should be created with debug exporter",
                   responseContent.contains("top_queries-"));
    }

    /**
     * Verify exported data structure
     */
    @SuppressWarnings("unchecked")
    protected void verifyExportedDataStructure() throws IOException {
        String indexName = getTopQueriesIndexName();
        if (indexName == null) {
            fail("No top_queries index found");
        }

        Request searchRequest = new Request("GET", "/" + indexName + "/_search?size=1");
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        // Parse response and verify structure
        try (XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseBody)) {
            Map<String, Object> responseMap = parser.map();
            Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");

            if (!hitsList.isEmpty()) {
                Map<String, Object> firstHit = hitsList.get(0);
                Map<String, Object> source = (Map<String, Object>) firstHit.get("_source");

                // Verify required fields exist
                assertTrue("Expected 'id' field in exported data", source.containsKey("id"));
                assertTrue("Expected 'timestamp' field in exported data", source.containsKey("timestamp"));
                assertTrue("Expected 'node_id' field in exported data", source.containsKey("node_id"));
            }
        }
    }

    /**
     * Verify index mappings
     */
    protected void verifyIndexMappings() throws IOException {
        String indexName = getTopQueriesIndexName();
        if (indexName == null) {
            fail("No top_queries index found");
        }

        Request mappingRequest = new Request("GET", "/" + indexName + "/_mapping");
        Response response = client().performRequest(mappingRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        // Verify key mapping fields exist
        assertTrue("Expected 'id' field mapping", responseBody.contains("\"id\""));
        assertTrue("Expected 'timestamp' field mapping", responseBody.contains("\"timestamp\""));
        assertTrue("Expected 'node_id' field mapping", responseBody.contains("\"node_id\""));
        assertTrue("Expected 'measurements' field mapping", responseBody.contains("\"measurements\""));
    }

    /**
     * Get the name of the top queries index
     */
    protected String getTopQueriesIndexName() throws IOException {
        Request indicesRequest = new Request("GET", "/_cat/indices?v");
        Response response = client().performRequest(indicesRequest);
        String responseContent = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        Pattern pattern = Pattern.compile("top_queries-(\\d{4}\\.\\d{2}\\.\\d{2}-\\d+)");
        Matcher matcher = pattern.matcher(responseContent);
        if (matcher.find()) {
            return "top_queries-" + matcher.group(1);
        }
        return null;
    }

    /**
     * Verify bulk export data
     */
    protected void verifyBulkExportData() throws IOException {
        String indexName = getTopQueriesIndexName();
        if (indexName == null) {
            fail("No top_queries index found");
        }

        Request searchRequest = new Request("GET", "/" + indexName + "/_search?size=0");
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected bulk export data to be present", responseBody.contains("\"total\""));
    }

    /**
     * Verify multiple records were exported
     */
    @SuppressWarnings("unchecked")
    protected void verifyMultipleRecordsExported() throws IOException {
        String indexName = getTopQueriesIndexName();
        if (indexName == null) {
            fail("No top_queries index found");
        }

        Request searchRequest = new Request("GET", "/" + indexName + "/_search?size=100");
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseBody)) {
            Map<String, Object> responseMap = parser.map();
            Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");

            assertTrue("Expected multiple records to be exported", hitsList.size() > 0);
        }
    }

    /**
     * Test invalid exporter configuration
     */
    protected void testInvalidExporterConfiguration() throws IOException {
        // Test with invalid exporter type
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{ \"persistent\": { \"search.insights.top_queries.exporter.type\": \"invalid_type\" } }"
        );

        try {
            client().performRequest(request);
            fail("Should not succeed with invalid exporter type");
        } catch (ResponseException e) {
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        }
    }

    /**
     * Verify index template creation
     */
    protected void verifyIndexTemplateCreation() throws IOException {
        Request templateRequest = new Request("GET", "/_index_template/query_insights_top_queries_template");
        Response response = client().performRequest(templateRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected template to exist", responseBody.contains("query_insights_top_queries_template"));
        assertTrue("Expected index patterns", responseBody.contains("top_queries-*"));
    }

    /**
     * Verify index template priority
     */
    @SuppressWarnings("unchecked")
    protected void verifyIndexTemplatePriority() throws IOException {
        Request templateRequest = new Request("GET", "/_index_template/query_insights_top_queries_template");
        Response response = client().performRequest(templateRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseBody)) {
            Map<String, Object> responseMap = parser.map();
            List<Map<String, Object>> templates = (List<Map<String, Object>>) responseMap.get("index_templates");

            if (!templates.isEmpty()) {
                Map<String, Object> template = templates.get(0);
                Map<String, Object> indexTemplate = (Map<String, Object>) template.get("index_template");
                Object priority = indexTemplate.get("priority");

                assertNotNull("Expected template priority to be set", priority);
                assertTrue("Expected priority to be a number", priority instanceof Number);
            }
        }
    }

    /**
     * Verify custom template priority
     */
    @SuppressWarnings("unchecked")
    protected void verifyCustomTemplatePriority(long expectedPriority) throws IOException {
        Request templateRequest = new Request("GET", "/_index_template/query_insights_top_queries_template");
        Response response = client().performRequest(templateRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseBody)) {
            Map<String, Object> responseMap = parser.map();
            List<Map<String, Object>> templates = (List<Map<String, Object>>) responseMap.get("index_templates");

            if (!templates.isEmpty()) {
                Map<String, Object> template = templates.get(0);
                Map<String, Object> indexTemplate = (Map<String, Object>) template.get("index_template");
                Object priority = indexTemplate.get("priority");

                assertNotNull("Expected template priority to be set", priority);
                assertEquals("Expected custom priority value", expectedPriority, ((Number) priority).longValue());
            }
        }
    }

    /**
     * Verify concurrent export results
     */
    protected void verifyConcurrentExportResults() throws IOException {
        String indexName = getTopQueriesIndexName();
        if (indexName == null) {
            fail("No top_queries index found");
        }

        Request searchRequest = new Request("GET", "/" + indexName + "/_search?size=100");
        Response response = client().performRequest(searchRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());

        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue("Expected concurrent export results", responseBody.contains("\"total\""));

        // Verify that we have results from concurrent operations
        assertTrue("Expected search results from concurrent operations", responseBody.contains("\"hits\""));
    }

    /**
     * Override cleanup to properly handle template deletion
     */
    @Override
    protected void cleanup() throws IOException, InterruptedException {
        Thread.sleep(12000);

        // Delete the specific template
        try {
            client().performRequest(new Request("DELETE", "/_index_template/query_insights_top_queries_template"));
        } catch (ResponseException e) {
            // Template might not exist, which is fine
        }

        // Delete top_queries indices with pattern
        try {
            client().performRequest(new Request("DELETE", "/top_queries-*"));
        } catch (ResponseException e) {
            // Indices might not exist, which is fine
        }

        try {
            client().performRequest(new Request("DELETE", "/my-index-0"));
        } catch (ResponseException e) {
            // Index might not exist, which is fine
        }

        String resetSettings = "{ \"persistent\": { "
            + "\"search.insights.top_queries.exporter.type\": \"none\", "
            + "\"search.insights.top_queries.latency.enabled\": \"false\" } }";
        Request resetReq = new Request("PUT", "/_cluster/settings");
        resetReq.setJsonEntity(resetSettings);
        client().performRequest(resetReq);
    }
}
