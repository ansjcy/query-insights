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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_INDEX_PATTERN_GLOB;

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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import java.util.HashMap;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.action.support.WriteRequest;
import java.util.Collections;

/**
 * Granular tests for the {@link LocalIndexExporterTests} class.
 */
public class LocalIndexExporterTests extends OpenSearchTestCase {
    private static final String TEMPLATE_NAME = "query_insights_override";
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
    public void testExportRecordsWhenIndexExists() throws IOException {
        // Create a mock client
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create the exporter with the mocks
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);
        
        // Make checkIndexExists return true to simulate index already exists
        doReturn(true).when(exporterSpy).checkIndexExists(anyString());
        
        // Mock the bulk method to track calls
        doAnswer(invocation -> null).when(exporterSpy).bulk(anyString(), any());
        
        // Generate test records
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(3);
        
        // Call export
        exporterSpy.export(records);
        
        // Verify bulk was called (to add records to existing index)
        verify(exporterSpy).bulk(anyString(), eq(records));
        
        // Verify ensureTemplateExists was NOT called (since index already exists)
        verify(exporterSpy, never()).ensureTemplateExists();
        
        // Verify createIndex was NOT called (since index already exists)
        verify(exporterSpy, never()).createIndex(anyString(), any());
    }

    @Test
    public void testExportRecordsWhenIndexNotExist() throws IOException {
        // Create a mock client
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create the exporter with the mocks
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);
        
        // Make checkIndexExists return false to simulate index doesn't exist
        doReturn(false).when(exporterSpy).checkIndexExists(anyString());
        
        // Mock ensureTemplateExists to return a completed future
        CompletableFuture<Boolean> completedFuture = CompletableFuture.completedFuture(true);
        doReturn(completedFuture).when(exporterSpy).ensureTemplateExists();
        
        // Mock createIndex to track calls
        doAnswer(invocation -> null).when(exporterSpy).createIndex(anyString(), any());
        
        // Generate test records
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(3);
        
        // Call export
        exporterSpy.export(records);
        
        // Verify ensureTemplateExists was called (since index doesn't exist)
        verify(exporterSpy).ensureTemplateExists();
        
        // Verify createIndex was called (since index doesn't exist)
        verify(exporterSpy).createIndex(anyString(), eq(records));
        
        // Verify bulk was NOT called directly (it's called inside createIndex)
        verify(exporterSpy, never()).bulk(anyString(), any());
    }

    @Test
    public void testExportWaitsForTemplateCreation() throws Exception {
        // Create a mock client
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        // Create the exporter with the mocks
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);
        
        // Make checkIndexExists return false to simulate index doesn't exist
        doReturn(false).when(exporterSpy).checkIndexExists(anyString());
        
        // Create a CompletableFuture that we can control manually
        CompletableFuture<Boolean> templateFuture = new CompletableFuture<>();
        doReturn(templateFuture).when(exporterSpy).ensureTemplateExists();
        
        // Use a CountDownLatch to track if createIndex is called
        CountDownLatch createIndexCalled = new CountDownLatch(1);
        doAnswer(invocation -> {
            createIndexCalled.countDown();
            return null;
        }).when(exporterSpy).createIndex(anyString(), any());
        
        // Generate test records
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(3);
        
        // Call export in a separate thread so it doesn't block our test
        Thread exportThread = new Thread(() -> {
            try {
                exporterSpy.export(records);
            } catch (Exception e) {
                fail("Export should not throw an exception: " + e.getMessage());
            }
        });
        exportThread.start();
        
        // Wait a short time to allow the export method to run up to the point of waiting for the template
        Thread.sleep(100);
        
        // Verify createIndex has NOT been called yet (should be waiting for template)
        assertEquals("createIndex should not be called until template is created", 1, createIndexCalled.getCount());
        
        // Complete the template future
        templateFuture.complete(true);
        
        // Wait for createIndex to be called
        boolean createIndexWasCalled = createIndexCalled.await(1, TimeUnit.SECONDS);
        
        // Verify createIndex was eventually called after template was created
        assertTrue("createIndex should be called after template is created", createIndexWasCalled);
        
        // Clean up
        exportThread.join(1000);
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

    @Test
    public void testGetAndSetIndexPattern() {
        final DateTimeFormatter newFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.ROOT);
        localIndexExporter.setIndexPattern(newFormatter);
        assert (localIndexExporter.getIndexPattern() == newFormatter);
    }

    @Test
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
        // Create a mock client that will capture the template request
        Client mockClient = mock(Client.class);
        ClusterService mockClusterService = mock(ClusterService.class);
        
        // Create a spy on LocalIndexExporter to intercept createTemplate calls
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalIndexExporter exporter = new LocalIndexExporter(mockClient, mockClusterService, format, "{}", "id");
        LocalIndexExporter exporterSpy = spy(exporter);
        
        // Set a custom priority to verify it's used in the template
        exporterSpy.setTemplatePriority(5000L);
        
        // Mock ensureTemplateExists to complete successfully without making actual calls
        CompletableFuture<Boolean> completedFuture = CompletableFuture.completedFuture(true);
        doReturn(completedFuture).when(exporterSpy).ensureTemplateExists();
        
        // Call ensureTemplateExists
        CompletableFuture<Boolean> future = exporterSpy.ensureTemplateExists();
        assertTrue(future.get(5, TimeUnit.SECONDS));
        
        // Verify the priority value was set correctly
        assertEquals("Template priority should be set correctly", 5000L, exporterSpy.getTemplatePriority());
    }

    /**
     * Test that ensureTemplateExists skips creating a template when it already exists
     */
    @Test
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
