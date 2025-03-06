/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
        indexName = format.format(ZonedDateTime.now(ZoneOffset.UTC))
            + "-"
            + generateLocalIndexDateHash(ZonedDateTime.now(ZoneOffset.UTC).toLocalDate());
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, 1 + randomInt(3), randomInt(2));
        clusterService = ClusterServiceUtils.createClusterService(threadPool, state.getNodes().getLocalNode(), clusterSettings);

        RoutingTable.Builder routingTable = RoutingTable.builder(state.routingTable());
        routingTable.addAsRecovery(
            IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build()
        );
        ClusterState updatedState = ClusterState.builder(state).routingTable(routingTable.build()).build();
        ClusterServiceUtils.setState(clusterService, updatedState);
        localIndexExporter = new LocalIndexExporter(client, clusterService, format, "", "id");

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
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

    @SuppressWarnings("unchecked")
    public void testExportRecordsWhenIndexExists() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doAnswer(invocation -> future).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
        assertEquals(2, bulkRequestBuilder.numberOfActions());
    }

    public void testExportRecordsWhenIndexNotExist() {
        RoutingTable.Builder routingTable = RoutingTable.builder();
        routingTable.addAsRecovery(
            IndexMetadata.builder("another_index")
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build()
        );
        ClusterState updatedState = ClusterState.builder(clusterService.state()).routingTable(routingTable.build()).build();
        ClusterServiceUtils.setState(clusterService, updatedState);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            localIndexExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
        verify(indicesAdminClient, times(1)).create(any(), any());
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

    public void testGetAndSetTemplateOrder() {
        final int newTemplateOrder = 1000;
        localIndexExporter.setTemplateOrder(newTemplateOrder);
        assertEquals(newTemplateOrder, localIndexExporter.getTemplateOrder());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testFindConflictingTemplatesV1() throws Exception {
        // Create a mock ClusterState with V1 template that would conflict with our pattern
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        Map<String, org.opensearch.cluster.metadata.IndexTemplateMetadata> templates = mock(Map.class);
        
        // Mock a conflicting template
        org.opensearch.cluster.metadata.IndexTemplateMetadata mockTemplate = mock(org.opensearch.cluster.metadata.IndexTemplateMetadata.class);
        when(mockTemplate.patterns()).thenReturn(List.of("*"));
        when(mockTemplate.order()).thenReturn(500);
        
        // Set up the mocked map to return our template
        when(templates.forEach(any())).thenAnswer(invocation -> {
            java.util.function.BiConsumer consumer = invocation.getArgument(0);
            consumer.accept("conflicting_template", mockTemplate);
            return null;
        });
        
        // Connect the mocks
        when(metadata.templates()).thenReturn(templates);
        when(metadata.templatesV2()).thenReturn(Map.of());
        when(mockState.getMetadata()).thenReturn(metadata);
        
        // Create a ClusterService that returns our mock state
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create a LocalIndexExporter with our mock ClusterService
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "", "id");
        
        // Use reflection to access private method findConflictingTemplatesWithDetails
        java.lang.reflect.Method findConflictingTemplatesMethod = 
            LocalIndexExporter.class.getDeclaredMethod("findConflictingTemplatesWithDetails");
        findConflictingTemplatesMethod.setAccessible(true);
        
        // Call the method and verify results
        Object result = findConflictingTemplatesMethod.invoke(exporter);
        
        assertNotNull(result);
        assertTrue(result instanceof LocalIndexExporter.TemplateConflictInfo);
        LocalIndexExporter.TemplateConflictInfo conflictInfo = (LocalIndexExporter.TemplateConflictInfo) result;
        
        assertFalse(conflictInfo.getConflictingTemplates().isEmpty());
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("conflicting_template"));
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("V1"));
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("order: 500"));
        assertEquals(500, conflictInfo.getHighestOrder());
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testFindConflictingTemplatesV2() throws Exception {
        // Create a mock ClusterState with V2 template that would conflict with our pattern
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        Map<String, org.opensearch.cluster.metadata.ComposableIndexTemplate> templatesV2 = mock(Map.class);
        
        // Mock a conflicting V2 template (ComposableIndexTemplate)
        org.opensearch.cluster.metadata.ComposableIndexTemplate mockTemplate = 
            mock(org.opensearch.cluster.metadata.ComposableIndexTemplate.class);
        when(mockTemplate.indexPatterns()).thenReturn(List.of("top_queries-*"));
        when(mockTemplate.priority()).thenReturn(800);
        
        // Set up the mocked map to return our V2 template
        when(templatesV2.forEach(any())).thenAnswer(invocation -> {
            java.util.function.BiConsumer consumer = invocation.getArgument(0);
            consumer.accept("conflicting_template_v2", mockTemplate);
            return null;
        });
        
        // Connect the mocks
        when(metadata.templates()).thenReturn(Map.of());
        when(metadata.templatesV2()).thenReturn(templatesV2);
        when(mockState.getMetadata()).thenReturn(metadata);
        
        // Create a ClusterService that returns our mock state
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create a LocalIndexExporter with our mock ClusterService
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "", "id");
        
        // Use reflection to access private method findConflictingTemplatesWithDetails
        java.lang.reflect.Method findConflictingTemplatesMethod = 
            LocalIndexExporter.class.getDeclaredMethod("findConflictingTemplatesWithDetails");
        findConflictingTemplatesMethod.setAccessible(true);
        
        // Call the method and verify results
        Object result = findConflictingTemplatesMethod.invoke(exporter);
        
        assertNotNull(result);
        assertTrue(result instanceof LocalIndexExporter.TemplateConflictInfo);
        LocalIndexExporter.TemplateConflictInfo conflictInfo = (LocalIndexExporter.TemplateConflictInfo) result;
        
        assertFalse(conflictInfo.getConflictingTemplates().isEmpty());
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("conflicting_template_v2"));
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("V2"));
        assertTrue(conflictInfo.getConflictingTemplates().get(0).contains("priority: 800"));
        assertEquals(800, conflictInfo.getHighestOrder());
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testGetAdjustedTemplateOrder() throws Exception {
        // Create a mock ClusterState with template that would have a high order
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        Map<String, org.opensearch.cluster.metadata.IndexTemplateMetadata> templates = mock(Map.class);
        
        // Mock a conflicting template with high order
        org.opensearch.cluster.metadata.IndexTemplateMetadata mockTemplate = mock(org.opensearch.cluster.metadata.IndexTemplateMetadata.class);
        when(mockTemplate.patterns()).thenReturn(List.of("*"));
        when(mockTemplate.order()).thenReturn(5000);
        
        // Set up the mocked map to return our template
        when(templates.forEach(any())).thenAnswer(invocation -> {
            java.util.function.BiConsumer consumer = invocation.getArgument(0);
            consumer.accept("high_order_template", mockTemplate);
            return null;
        });
        
        // Connect the mocks
        when(metadata.templates()).thenReturn(templates);
        when(metadata.templatesV2()).thenReturn(Map.of());
        when(mockState.getMetadata()).thenReturn(metadata);
        
        // Create a ClusterService that returns our mock state
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create a LocalIndexExporter with our mock ClusterService and set initial template order
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "", "id");
        exporter.setTemplateOrder(1000); // Set initial order lower than the conflicting template
        
        // Use reflection to access private method getAdjustedTemplateOrder
        java.lang.reflect.Method getAdjustedTemplateOrderMethod = 
            LocalIndexExporter.class.getDeclaredMethod("getAdjustedTemplateOrder");
        getAdjustedTemplateOrderMethod.setAccessible(true);
        
        // Call the method and verify results
        int adjustedOrder = (int) getAdjustedTemplateOrderMethod.invoke(exporter);
        
        // The adjusted order should be higher than the conflicting template's order
        assertTrue(adjustedOrder > 5000);
        // Specifically, it should be the conflicting order + 100
        assertEquals(5100, adjustedOrder);
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testGetAdjustedTemplateOrderWithVeryHighOrder() throws Exception {
        // Create a mock ClusterState with template that would have a very high order
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        Map<String, org.opensearch.cluster.metadata.IndexTemplateMetadata> templates = mock(Map.class);
        
        // Mock a conflicting template with order close to Integer.MAX_VALUE
        org.opensearch.cluster.metadata.IndexTemplateMetadata mockTemplate = mock(org.opensearch.cluster.metadata.IndexTemplateMetadata.class);
        when(mockTemplate.patterns()).thenReturn(List.of("*"));
        when(mockTemplate.order()).thenReturn(Integer.MAX_VALUE - 500); // Very high order
        
        // Set up the mocked map to return our template
        when(templates.forEach(any())).thenAnswer(invocation -> {
            java.util.function.BiConsumer consumer = invocation.getArgument(0);
            consumer.accept("very_high_order_template", mockTemplate);
            return null;
        });
        
        // Connect the mocks
        when(metadata.templates()).thenReturn(templates);
        when(metadata.templatesV2()).thenReturn(Map.of());
        when(mockState.getMetadata()).thenReturn(metadata);
        
        // Create a ClusterService that returns our mock state
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create a LocalIndexExporter with our mock ClusterService
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "", "id");
        
        // Use reflection to access private method getAdjustedTemplateOrder
        java.lang.reflect.Method getAdjustedTemplateOrderMethod = 
            LocalIndexExporter.class.getDeclaredMethod("getAdjustedTemplateOrder");
        getAdjustedTemplateOrderMethod.setAccessible(true);
        
        // Call the method and verify results
        int adjustedOrder = (int) getAdjustedTemplateOrderMethod.invoke(exporter);
        
        // The adjusted order should not exceed Integer.MAX_VALUE - 1000
        assertEquals(Integer.MAX_VALUE - 1000, adjustedOrder);
    }

    /**
     * Test that ensureTemplateExists creates a V2 template correctly
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testEnsureTemplateExistsCreatesV2Template() throws Exception {
        // Create mocks for the test
        ClusterState mockState = mock(ClusterState.class);
        org.opensearch.cluster.metadata.Metadata metadata = mock(org.opensearch.cluster.metadata.Metadata.class);
        
        // Set up the mock state to have empty templates
        when(metadata.templates()).thenReturn(Map.of());
        when(metadata.templatesV2()).thenReturn(Map.of());
        when(mockState.getMetadata()).thenReturn(metadata);
        
        // Create a ClusterService that returns our mock state
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(mockState);
        
        // Create mocks for the admin client
        AdminClient mockAdminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(mockAdminClient);
        
        // Create a mock of the execute method to verify it's called with the right action and request
        org.mockito.ArgumentCaptor<org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request> requestCaptor = 
            org.mockito.ArgumentCaptor.forClass(org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request.class);

        // Stub the execute method 
        doAnswer(invocation -> {
            // Get the ActionListener from the third argument
            ActionListener listener = invocation.getArgument(2);
            // Call onResponse with mock response
            org.opensearch.action.support.AcknowledgedResponse mockResponse = mock(org.opensearch.action.support.AcknowledgedResponse.class);
            when(mockResponse.isAcknowledged()).thenReturn(true);
            listener.onResponse(mockResponse);
            return null;
        }).when(client).execute(
            org.mockito.ArgumentMatchers.eq(org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.INSTANCE),
            requestCaptor.capture(),
            org.mockito.ArgumentMatchers.any()
        );
        
        // Create a LocalIndexExporter with our mock components
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "{}", "id");
        
        // Call the ensureTemplateExists method using reflection
        java.lang.reflect.Method ensureTemplateExistsMethod = 
            LocalIndexExporter.class.getDeclaredMethod("ensureTemplateExists");
        ensureTemplateExistsMethod.setAccessible(true);
        ensureTemplateExistsMethod.invoke(exporter);
        
        // Verify that the execute method was called
        org.mockito.Mockito.verify(client).execute(
            org.mockito.ArgumentMatchers.eq(org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.INSTANCE),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any()
        );
        
        // Verify the request
        org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request capturedRequest = requestCaptor.getValue();
        assertNotNull(capturedRequest);
        assertEquals("query_insights_override", capturedRequest.name());
    }
