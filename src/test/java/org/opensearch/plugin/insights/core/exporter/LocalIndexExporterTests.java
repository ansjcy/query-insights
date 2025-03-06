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

    public void testGetAndSetTemplatePriority() {
        final long newTemplatePriority = 2000L;
        localIndexExporter.setTemplatePriority(newTemplatePriority);
        assertEquals(newTemplatePriority, localIndexExporter.getTemplatePriority());
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
        
        // Create a LocalIndexExporter with our mock components and a custom priority
        LocalIndexExporter exporter = new LocalIndexExporter(client, mockClusterService, format, "{}", "id");
        exporter.setTemplatePriority(5000L);
        
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
        
        // Verify the request has the right name
        org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.Request capturedRequest = requestCaptor.getValue();
        assertNotNull(capturedRequest);
        assertEquals("query_insights_override", capturedRequest.name());
        
        // Verify the template has the right priority (needs additional mocking/reflection)
        if (capturedRequest.indexTemplate() != null) {
            // Use reflection to access the private field in the request
            java.lang.reflect.Field templateField = capturedRequest.getClass().getDeclaredField("indexTemplate");
            templateField.setAccessible(true);
            org.opensearch.cluster.metadata.ComposableIndexTemplate template = 
                (org.opensearch.cluster.metadata.ComposableIndexTemplate) templateField.get(capturedRequest);
            
            assertEquals(Long.valueOf(5000L), template.priority());
        }
    }
