/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.insights.core.service.grouper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.QueryInsightsRestTestCase;

/**
 * ITs for Grouping Top Queries by none
 */
public class MinMaxQueryGrouperByNoneIT extends QueryInsightsRestTestCase {

    /**
     * Grouping by none should not group queries
     * @throws IOException
     * @throws InterruptedException
     */
    public void testGroupingByNone() throws IOException, InterruptedException {

        updateClusterSettings(this::groupByNoneSettings);

        waitForEmptyTopQueriesResponse();

        // Search
        doSearch("range", 2);
        doSearch("match", 6);
        doSearch("term", 4);

        assertTopQueriesCount(12, "latency");
    }

    /**
     * Test that identical queries are not grouped when grouping is disabled
     * @throws IOException
     * @throws InterruptedException
     */
    public void testIdenticalQueriesNotGrouped() throws IOException, InterruptedException {
        updateClusterSettings(this::groupByNoneSettings);
        waitForEmptyTopQueriesResponse();

        // Execute identical queries multiple times
        doSearch("match", 5);
        doSearch("match", 5);

        // Should have 10 separate query records, not grouped
        assertTopQueriesCount(10, "latency");

        // Verify no aggregation type is set (should be NONE)
        String responseBody = getTopQueries("latency");
        verifyNoAggregation(responseBody);
    }

    /**
     * Test ungrouped queries with multiple metric types
     * @throws IOException
     * @throws InterruptedException
     */
    public void testUngroupedQueriesWithMultipleMetrics() throws IOException, InterruptedException {
        updateClusterSettings(this::multiMetricNoneGroupingSettings);
        waitForEmptyTopQueriesResponse();

        // Execute queries
        doSearch("match", 3);
        doSearch("range", 3);

        // Each metric type should have 6 individual queries
        assertTopQueriesCount(6, "latency");
        assertTopQueriesCount(6, "cpu");
        assertTopQueriesCount(6, "memory");
    }

    /**
     * Test performance with high volume of ungrouped queries
     * @throws IOException
     * @throws InterruptedException
     */
    public void testHighVolumeUngroupedQueries() throws IOException, InterruptedException {
        updateClusterSettings(this::groupByNoneSettings);
        waitForEmptyTopQueriesResponse();

        // Execute many queries to test performance
        doSearch("match", 20);

        // Should have all 20 individual queries
        assertTopQueriesCount(20, "latency");
    }

    private String groupByNoneSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.grouping.group_by\" : \"none\",\n"
            + "        \"search.insights.top_queries.grouping.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }

    private String multiMetricNoneGroupingSettings() {
        return "{\n"
            + "    \"persistent\" : {\n"
            + "        \"search.insights.top_queries.latency.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.cpu.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.memory.enabled\" : \"true\",\n"
            + "        \"search.insights.top_queries.latency.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.cpu.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.memory.window_size\" : \"1m\",\n"
            + "        \"search.insights.top_queries.latency.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.cpu.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.memory.top_n_size\" : 100,\n"
            + "        \"search.insights.top_queries.group_by\" : \"none\",\n"
            + "        \"search.insights.top_queries.max_groups_excluding_topn\" : 5\n"
            + "    }\n"
            + "}";
    }

    /**
     * Helper method to verify no aggregation is applied
     */
    private void verifyNoAggregation(String responseBody) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                responseBody.getBytes(StandardCharsets.UTF_8)
            )
        ) {
            Map<String, Object> responseMap = parser.map();
            @SuppressWarnings("unchecked")
            Map<String, Object> topQueries = (Map<String, Object>) responseMap.get("top_queries");
            if (topQueries != null) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> queries = (List<Map<String, Object>>) topQueries.get("latency");
                if (queries != null && !queries.isEmpty()) {
                    Map<String, Object> firstQuery = queries.get(0);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> measurements = (Map<String, Object>) firstQuery.get("measurements");
                    if (measurements != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> latency = (Map<String, Object>) measurements.get("latency");
                        if (latency != null) {
                            String aggregationType = (String) latency.get("aggregationType");
                            assertEquals("Expected aggregation type to be NONE for ungrouped queries",
                                "NONE", aggregationType);
                        }
                    }
                }
            }
        }
    }
}
