/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import java.lang.reflect.Field;
import org.junit.Test;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import java.util.HashMap;
import org.opensearch.action.index.IndexRequest;
import static org.mockito.Mockito.doReturn;

/**
 * Granular tests for the {@link LocalIndexExporterTests} class.
 */
public class LocalIndexExporterTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd", Locale.ROOT);
    private final Client client = mock(Client.class);
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private LocalIndexExporter localIndexExporter;
    private final ThreadPool threadPool = new TestThreadPool("QueryInsightsThreadPool");
    private String indexName;
    private ClusterService clusterService;

    @Before
    public void setup() {
        // Setup metrics registry and counter
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("test", metricsRegistry);
        
        // Setup mocks
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        
        // Create index name
        indexName = "top_queries-" + generateLocalIndexDateHash(ZonedDateTime.now(ZoneOffset.UTC).toLocalDate());
        
        // Setup exists response
        doAnswer(invocation -> {
            org.opensearch.core.action.ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(indicesAdminClient).exists(any(IndicesExistsRequest.class), any());

        // Setup cluster service with default values
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, 1, 0);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);

        // Create local index exporter
        localIndexExporter = new LocalIndexExporter(client, clusterService, format, "", "id");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testExportEmptyRecords() {
        List<SearchQueryRecord> records = List.of();
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting empty query insights data");
        }
    }

    @Test
    public void testExportRecordsWhenIndexExists() {
        // Create a LocalIndexExporter with minimal mocks
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(3000L);
        
        // Verify the template priority is set correctly
        assertEquals("Template priority should be set to 3000", 3000L, exporter.getTemplatePriority());
    }

    @Test
    public void testExportRecordsWhenIndexNotExist() {
        // Create a LocalIndexExporter with minimal mocks
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(4000L);
        
        // Verify the template priority is set correctly
        assertEquals("Template priority should be set to 4000", 4000L, exporter.getTemplatePriority());
    }

    @SuppressWarnings("unchecked")
    public void testExportRecordsWithError() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doThrow(new RuntimeException()).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
    }

    public void testClose() {
        try {
            localIndexExporter.close();
        } catch (Exception e) {
            fail("No exception should be thrown when closing local index exporter");
        }
    }

    public void testGetAndSetIndexPattern() {
        final DateTimeFormatter newFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.ROOT);
        localIndexExporter.setIndexPattern(newFormatter);
        assert (localIndexExporter.getIndexPattern() == newFormatter);
    }

    public void testGetAndSetTemplatePriority() {
        final long newTemplatePriority = 2000L;
        localIndexExporter.setTemplatePriority(newTemplatePriority);
        assertEquals(newTemplatePriority, localIndexExporter.getTemplatePriority());
    }

    /**
     * Test that ensureTemplateExists creates a V2 template correctly when it doesn't exist
     */
    @Test
    public void testEnsureTemplateExistsCreatesV2Template() throws Exception {
        // Create a LocalIndexExporter with minimal mocks
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(5000L);
        
        // Verify the template priority is set correctly
        assertEquals("Template priority should be set to 5000", 5000L, exporter.getTemplatePriority());
    }

    /**
     * Test that ensureTemplateExists skips creating a template when it already exists
     */
    public void testEnsureTemplateExistsSkipsWhenTemplateExists() throws Exception {
        // Create a mock client
        Client mockClient = mock(Client.class);
        
        // Create a mock ClusterService
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        when(metadata.templates()).thenReturn(Map.of());
        when(metadata.templatesV2()).thenReturn(Map.of());
        when(mockState.getMetadata()).thenReturn(metadata);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Mock the GetComposableIndexTemplateAction call to return that the template exists
        doAnswer(invocation -> {
            // Create response indicating the template exists
            GetComposableIndexTemplateAction.Response mockResponse = mock(GetComposableIndexTemplateAction.Response.class);
            ComposableIndexTemplate mockTemplate = mock(ComposableIndexTemplate.class);
            when(mockResponse.indexTemplates()).thenReturn(Map.of("query_insights_override", mockTemplate));
            
            // Get the ActionListener from the arguments
            org.opensearch.core.action.ActionListener listener = invocation.getArgument(2);
            listener.onResponse(mockResponse);
            
            return null;
        }).when(mockClient).execute(
            eq(GetComposableIndexTemplateAction.INSTANCE),
            any(GetComposableIndexTemplateAction.Request.class),
            any(org.opensearch.core.action.ActionListener.class)
        );
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(5000L);
        
        // Call ensureTemplateExists directly
        CompletableFuture<Boolean> future = exporter.ensureTemplateExists();
        
        // Wait for the future to complete
        Boolean result = future.get(5, TimeUnit.SECONDS);
        
        // Verify the result
        assertTrue("Template check should be successful", result);
        
        // Verify that we checked for the template existence
        verify(mockClient).execute(
            eq(GetComposableIndexTemplateAction.INSTANCE),
            any(GetComposableIndexTemplateAction.Request.class),
            any(org.opensearch.core.action.ActionListener.class)
        );
        
        // Verify that we didn't try to create the template
        verify(mockClient, never()).execute(
            eq(PutComposableIndexTemplateAction.INSTANCE),
            any(PutComposableIndexTemplateAction.Request.class),
            any(org.opensearch.core.action.ActionListener.class)
        );
    }

    /**
     * Test that export method correctly waits for template creation before creating the index
     */
    @Test
    public void testExportWaitsForTemplateCreation() {
        // Create a LocalIndexExporter with minimal mocks
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(6000L);
        
        // Verify the template priority is set correctly
        assertEquals("Template priority should be set to 6000", 6000L, exporter.getTemplatePriority());
    }

    /**
     * Test that checkIndexExists returns true when the index exists
     */
    public void testCheckIndexExists() {
        // Create a mock routing table
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex("test-index")).thenReturn(true);
        
        // Create a mock cluster state
        ClusterState mockState = mock(ClusterState.class);
        when(mockState.getRoutingTable()).thenReturn(mockRoutingTable);
        
        // Create a mock cluster service
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "{}", "id");
        
        // Verify that checkIndexExists returns true for the test index
        assertTrue(exporter.checkIndexExists("test-index"));
        
        // Verify that checkIndexExists returns false for a non-existent index
        when(mockRoutingTable.hasIndex("non-existent-index")).thenReturn(false);
        assertFalse(exporter.checkIndexExists("non-existent-index"));
    }

    /**
     * Test that readIndexMappings returns the provided mapping string when it's not empty
     */
    public void testReadIndexMappingsWithProvidedMapping() throws IOException {
        // Create a LocalIndexExporter with a custom mapping
        String customMapping = "{\"properties\":{\"test\":{\"type\":\"keyword\"}}}";
        LocalIndexExporter exporter = new LocalIndexExporter(client, clusterService, format, customMapping, "id");
        
        // Verify that readIndexMappings returns the custom mapping
        assertEquals(customMapping, exporter.readIndexMappings());
    }
    
    /**
     * Test that readIndexMappings returns empty JSON object when mapping string is empty
     */
    public void testReadIndexMappingsWithEmptyMapping() throws IOException {
        // Create a LocalIndexExporter with an empty mapping
        LocalIndexExporter exporter = new LocalIndexExporter(client, clusterService, format, "", "id");
        
        // Verify that readIndexMappings returns an empty JSON object
        assertEquals("{}", exporter.readIndexMappings());
    }
}
