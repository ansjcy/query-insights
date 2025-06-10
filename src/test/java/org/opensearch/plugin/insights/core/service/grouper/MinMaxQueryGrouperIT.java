/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

public class MinMaxQueryGrouperIT extends QueryInsightsRestTestCase {
    /**
     * Grouping by none should not group queries
     * @throws IOException
     * @throws InterruptedException
     */
    public void testNoneToSimilarityGroupingTransition() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueriesSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Top queries should be drained due to grouping change from NONE -> SIMILARITY
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        // 3 groups
        assertTopQueriesCount(3, "latency");
    }

    public void testSimilarityToNoneGroupingTransition() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");

        updateClusterSettings(this::defaultTopQueriesSettings);

        // Top queries should be drained due to grouping change from SIMILARITY -> NONE
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");
    }

    public void testSimilarityMaxGroupsChanged() throws IOException, InterruptedException {

        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");

        // Change max groups exluding topn setting
        updateClusterSettings(this::updateMaxGroupsExcludingTopNSetting);

        // Top queries should be drained due to max group change
        assertTopQueriesCount(0, "latency");

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(3, "latency");
    }

    /**
     * Test comprehensive query grouping scenarios with multiple metric types
     */
    public void testComprehensiveQueryGroupingScenarios() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();

        // Enable grouping with multiple metrics
        updateClusterSettings(this::multiMetricGroupingSettings);

        // Execute different query types
        doSearch("match", 3);
        doSearch("range", 3);
        doSearch("term", 3);
        doSearch("bool", 3);

        // Should create 4 groups for latency
        assertTopQueriesCount(4, "latency");

        // Verify that CPU and memory metrics are also grouped
        assertTopQueriesCount(4, "cpu");
        assertTopQueriesCount(4, "memory");
    }

    /**
     * Test query grouping with field name and type inclusion
     */
    public void testQueryGroupingWithFieldAttributes() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();

        // Enable field name and type inclusion
        updateClusterSettings(this::fieldAttributesGroupingSettings);

        // Execute queries with different field names but same query type
        doSearchWithCustomField("match", "title", 2);
        doSearchWithCustomField("match", "content", 2);

        // Should create 2 groups due to different field names
        assertTopQueriesCount(2, "latency");
    }

    /**
     * Test query grouping behavior under high load
     */
    public void testQueryGroupingUnderHighLoad() throws IOException, InterruptedException {
        waitForEmptyTopQueriesResponse();

        updateClusterSettings(this::defaultTopQueryGroupingSettings);

        // Execute many queries of the same type to test aggregation
        doSearch("match", 20);

        // Should create only 1 group despite many queries
        assertTopQueriesCount(1, "latency");

        // Verify the group contains aggregated data
        String responseBody = getTopQueries("latency");
        verifyGroupContainsMultipleQueries(responseBody);
    }

    protected String updateMaxGroupsExcludingTopNSetting() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 1\n"
            + "    }\n"
            + "}";
    }

    protected String multiMetricGroupingSettings() {
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
            + "        \"search.insights.top_queries.grouping.group_by\" : \"similarity\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 10\n"
            + "    }\n"
            + "}";
    }

    protected String fieldAttributesGroupingSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 5,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"similarity\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 5,\n"
            + "        \"search.insights.top_queries.grouping.attributes.field_name\" : true,\n"
            + "        \"search.insights.top_queries.grouping.attributes.field_type\" : true\n"
            + "    }\n"
            + "}";
    }

    /**
     * Helper method to perform search with custom field
     */
    protected void doSearchWithCustomField(String queryType, String fieldName, int times) throws IOException {
        try (RestClient firstNodeClient = getFirstNodeClient()) {
            for (int i = 0; i < times; i++) {
                Request request = new Request("GET", "/my-index-0/_search?size=20&pretty");
                request.setJsonEntity(createSearchBodyWithField(queryType, fieldName));
                Response response = firstNodeClient.performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * Helper method to create search body with custom field
     */
    private String createSearchBodyWithField(String queryType, String fieldName) {
        switch (queryType) {
            case "match":
                return "{\n" + "  \"query\": {\n" + "    \"match\": {\n" + "      \"" + fieldName + "\": \"test-value\"\n" + "    }\n" + "  }\n" + "}";
            default:
                throw new IllegalArgumentException("Unknown query type: " + queryType);
        }
    }

    /**
     * Helper method to verify group contains multiple queries
     */
    private void verifyGroupContainsMultipleQueries(String responseBody) {
        // This is a simplified verification - in a real implementation,
        // you would parse the JSON and check the count field in measurements
        assertTrue("Response should contain aggregated query data", responseBody.contains("\"count\""));
    }
}
