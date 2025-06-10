/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.QUERY_INSIGHTS_INDEX_TAG_NAME;
import static org.opensearch.plugin.insights.core.service.QueryInsightsService.getInitialDelay;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_EXPORTER_ID;
import static org.opensearch.plugin.insights.core.service.TopQueriesService.TOP_QUERIES_INDEX_TAG_VALUE;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.ENTRY_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.EVICTIONS;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.HIT_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.MISS_COUNT;
import static org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator.SIZE_IN_BYTES;
import static org.opensearch.plugin.insights.core.utils.ExporterReaderUtils.generateLocalIndexDateHash;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.DebugExporter;
import org.opensearch.plugin.insights.core.exporter.LocalIndexExporter;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.core.exporter.SinkType;
import org.opensearch.plugin.insights.core.metrics.OperationalMetricsCounter;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReader;
import org.opensearch.plugin.insights.core.reader.QueryInsightsReaderFactory;
import org.opensearch.plugin.insights.core.service.categorizer.QueryShapeGenerator;
import org.opensearch.plugin.insights.rules.model.GroupingType;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.healthStats.QueryInsightsHealthStats;
import org.opensearch.plugin.insights.rules.model.healthStats.TopQueriesHealthStats;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;



/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);
    private ThreadPool threadPool;
    private final Client client = mock(Client.class);
    private final NamedXContentRegistry namedXContentRegistry = mock(NamedXContentRegistry.class);
    private QueryInsightsService queryInsightsService;
    private QueryInsightsService queryInsightsServiceSpy;
    private final AdminClient adminClient = mock(AdminClient.class);
    private final IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    private final ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
    private ClusterService clusterService;
    private LocalIndexExporter mockLocalIndexExporter;
    private DebugExporter mockDebugExporter;
    private QueryInsightsReader mockReader;
    private QueryInsightsExporterFactory queryInsightsExporterFactory;
    private QueryInsightsReaderFactory queryInsightsReaderFactory;

    @Before
    public void setup() {
        queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);
        queryInsightsReaderFactory = mock(QueryInsightsReaderFactory.class);
        mockLocalIndexExporter = mock(LocalIndexExporter.class);
        mockDebugExporter = mock(DebugExporter.class);
        mockReader = mock(QueryInsightsReader.class);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        this.threadPool = new TestThreadPool(
            "QueryInsightsHealthStatsTests",
            new ScalingExecutorBuilder(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR, 1, 5, TimeValue.timeValueMinutes(5))
        );
        clusterService = new ClusterService(settings, clusterSettings, threadPool);
        queryInsightsService = new QueryInsightsService(
            clusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry,
            queryInsightsExporterFactory,
            queryInsightsReaderFactory
        );
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.MEMORY, true);
        queryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(clusterService));
        queryInsightsServiceSpy = spy(queryInsightsService);

        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        OperationalMetricsCounter.initialize("cluster", metricsRegistry);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (clusterService != null) {
            IOUtils.close(clusterService);
        }
        if (queryInsightsService != null) {
            queryInsightsService.doClose();
        }

        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * Helper method to clean up services created in individual tests
     */
    private void cleanupTestServices(List<AbstractLifecycleComponent> services) throws IOException {
        if (services != null && services.size() >= 2) {
            QueryInsightsService service = (QueryInsightsService) services.get(0);
            ClusterService clusterService = (ClusterService) services.get(1);

            if (service != null) {
                service.doStop();
            }
            if (clusterService != null) {
                IOUtils.close(clusterService);
            }
            if (service != null) {
                service.doClose();
            }
        }
    }

    public void testAddRecordToLimitAndDrain() {
        SearchQueryRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1, 1, System.currentTimeMillis(), 0).get(0);
        for (int i = 0; i < QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY; i++) {
            assertTrue(queryInsightsService.addRecord(record));
        }
        // exceed capacity
        assertFalse(queryInsightsService.addRecord(record));
        queryInsightsService.drainRecords();
        assertEquals(
            QueryInsightsSettings.DEFAULT_TOP_N_SIZE,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
    }

    public void testDoStart() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.getFirst();
        try {
            updatedQueryInsightsService.doStart();
        } catch (Exception e) {
            fail(String.format(Locale.ROOT, "No exception expected when starting query insights service: %s", e.getMessage()));
        }
        assertEquals(1, updatedQueryInsightsService.scheduledFutures.size());
        assertFalse(updatedQueryInsightsService.scheduledFutures.getFirst().isCancelled());
        assertNotNull(updatedQueryInsightsService.deleteIndicesScheduledFuture);
        assertFalse(updatedQueryInsightsService.deleteIndicesScheduledFuture.isCancelled());

        cleanupTestServices(updatedService);
    }

    public void testClose() {
        try {
            queryInsightsService.doClose();
        } catch (Exception e) {
            fail("No exception expected when closing query insights service");
        }
    }

    public void testSearchQueryMetricsEnabled() {
        // Initially, searchQueryMetricsEnabled should be false and searchQueryCategorizer should be null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Enable search query metrics
        queryInsightsService.enableSearchQueryMetricsFeature(true);

        // Assert that searchQueryMetricsEnabled is true and searchQueryCategorizer is initialized
        assertTrue(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

        // Disable search query metrics
        queryInsightsService.enableSearchQueryMetricsFeature(false);

        // Assert that searchQueryMetricsEnabled is false and searchQueryCategorizer is not null
        assertFalse(queryInsightsService.isSearchQueryMetricsFeatureEnabled());
        assertNotNull(queryInsightsService.getSearchQueryCategorizer());

    }

    public void testAddRecordGroupBySimilarityWithDifferentGroups() {

        int numberOfRecordsRequired = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightsRecordsWithMeasurement(
            numberOfRecordsRequired,
            MetricType.LATENCY,
            5
        );

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < numberOfRecordsRequired; i++) {
            assertTrue(queryInsightsService.addRecord(records.get(i)));
        }
        // exceed capacity but handoff to grouping
        assertTrue(queryInsightsService.addRecord(records.get(numberOfRecordsRequired - 1)));

        queryInsightsService.drainRecords();

        assertEquals(
            QueryInsightsSettings.DEFAULT_TOP_N_SIZE,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
    }

    public void testAddRecordGroupBySimilarityWithOneGroup() {
        int numberOfRecordsRequired = 10;
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightsRecordsWithMeasurement(
            numberOfRecordsRequired,
            MetricType.LATENCY,
            5
        );
        QueryInsightsTestUtils.populateSameQueryHashcodes(records);

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < numberOfRecordsRequired; i++) {
            assertTrue(queryInsightsService.addRecord(records.get(i)));
        }
        // exceed capacity but handoff to grouping service
        assertTrue(queryInsightsService.addRecord(records.get(numberOfRecordsRequired - 1)));

        queryInsightsService.drainRecords();
        assertEquals(
            1,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
    }

    public void testAddRecordGroupBySimilarityWithTwoGroups() {
        List<SearchQueryRecord> records1 = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, System.currentTimeMillis(), 0);
        QueryInsightsTestUtils.populateHashcode(records1, 1);

        List<SearchQueryRecord> records2 = QueryInsightsTestUtils.generateQueryInsightRecords(2, 2, System.currentTimeMillis(), 0);
        QueryInsightsTestUtils.populateHashcode(records2, 2);

        queryInsightsService.setGrouping(GroupingType.SIMILARITY.getValue());
        assertEquals(queryInsightsService.getGrouping(), GroupingType.SIMILARITY);

        for (int i = 0; i < 2; i++) {
            assertTrue(queryInsightsService.addRecord(records1.get(i)));
            assertTrue(queryInsightsService.addRecord(records2.get(i)));
        }

        queryInsightsService.drainRecords();
        assertEquals(
            2,
            queryInsightsService.getTopQueriesService(MetricType.LATENCY).getTopQueriesRecords(false, null, null, null, null).size()
        );
    }

    public void testGetHealthStats() {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        queryInsightsService.addRecord(records.get(0));
        QueryInsightsHealthStats healthStats = queryInsightsService.getHealthStats();
        assertNotNull(healthStats);
        assertEquals(threadPool.info(QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR), healthStats.getThreadPoolInfo());
        assertEquals(1, healthStats.getQueryRecordsQueueSize());
        Map<MetricType, TopQueriesHealthStats> topQueriesHealthStatsMap = healthStats.getTopQueriesHealthStats();
        assertNotNull(topQueriesHealthStatsMap);
        assertEquals(3, topQueriesHealthStatsMap.size());
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.LATENCY));
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.CPU));
        assertTrue(topQueriesHealthStatsMap.containsKey(MetricType.MEMORY));
        Map<String, Long> fieldTypeCacheStats = healthStats.getFieldTypeCacheStats();
        assertNotNull(fieldTypeCacheStats);
        assertEquals(5, fieldTypeCacheStats.size());
        assertTrue(fieldTypeCacheStats.containsKey(SIZE_IN_BYTES));
        assertTrue(fieldTypeCacheStats.containsKey(ENTRY_COUNT));
        assertTrue(fieldTypeCacheStats.containsKey(EVICTIONS));
        assertTrue(fieldTypeCacheStats.containsKey(HIT_COUNT));
        assertTrue(fieldTypeCacheStats.containsKey(MISS_COUNT));
    }

    public void testDeleteAllTopNIndices() throws IOException {
        // Create 9 top_queries-* indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            String indexName = "top_queries-2024.01.0"
                + i
                + "-"
                + generateLocalIndexDateHash(ZonedDateTime.of(2024, 1, i, 0, 0, 0, 0, ZoneId.of("UTC")).toLocalDate());
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }
        // Create 5 user indices
        for (int i = 0; i < 5; i++) {
            String indexName = "my_index-" + i;
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        updatedQueryInsightsService.deleteAllTopNIndices(client, mockLocalIndexExporter);
        // All 10 top_queries-* indices should be deleted, while none of the users indices should be deleted
        verify(mockLocalIndexExporter, times(9)).deleteSingleIndex(argThat(str -> str.matches("top_queries-.*")), any());

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndices() throws InterruptedException, IOException {
        // Test with a new cluster state with expired index mappings
        // Create 9 top_queries-* indices with creation dates older than the retention period
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            LocalDate date = LocalDate.of(2023, 1, i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, creationTime)
                )
                .putMapping(
                    new MappingMetadata("_doc", Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE)))
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }
        // Create some non Query Insights indices
        for (String indexName : List.of("logs-1", "logs-2", "top_queries-2023.01.01-12345", "top_queries-2023.01.02-12345")) {
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(SETTING_CREATION_DATE, Instant.now().minus(100, ChronoUnit.DAYS).toEpochMilli())
                )
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);
        final int expectedIndicesDeleted = 2;
        CountDownLatch latch = new CountDownLatch(expectedIndicesDeleted);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(indicesAdminClient).delete(any(), any());
        // Call the method under test
        updatedQueryInsightsService.deleteExpiredTopNIndices();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        // Verify that the correct number of indices are deleted
        // Default retention is 7 days, so oldest 2 indices should be deleted
        verify(client, times(expectedIndicesDeleted + 1)).admin(); // one extra to get list of local indices
        verify(adminClient, times(expectedIndicesDeleted)).indices();
        verify(indicesAdminClient, times(expectedIndicesDeleted)).delete(any(), any());

        cleanupTestServices(updatedService);
    }

    public void testValidateExporterDeleteAfter() {
        this.queryInsightsService.validateExporterDeleteAfter(7);
        this.queryInsightsService.validateExporterDeleteAfter(180);
        this.queryInsightsService.validateExporterDeleteAfter(1);
        assertThrows(IllegalArgumentException.class, () -> { this.queryInsightsService.validateExporterDeleteAfter(-1); });
        assertThrows(IllegalArgumentException.class, () -> { this.queryInsightsService.validateExporterDeleteAfter(0); });
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            this.queryInsightsService.validateExporterDeleteAfter(181);
        });
        assertEquals(
            "Invalid exporter delete_after_days setting [181], value should be an integer between 1 and 180.",
            exception.getMessage()
        );
    }

    public void testSetExporterAndReaderType_SwitchFromLocalIndexToNone() throws IOException {
        // Mock current exporter and reader
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        doNothing().when(queryInsightsServiceSpy).deleteAllTopNIndices(any(), any());
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.NONE);
        // Verify cleanup of local indices
        verify(queryInsightsServiceSpy, times(1)).deleteAllTopNIndices(client, mockLocalIndexExporter);
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        // Verify exporter is set to NONE
        assertEquals(SinkType.NONE, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromLocalIndexToDebug() throws IOException {
        // Mock current exporter and reader
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        doNothing().when(queryInsightsServiceSpy).deleteAllTopNIndices(any(), any());
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);

        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify cleanup of local indices
        verify(queryInsightsServiceSpy, times(1)).deleteAllTopNIndices(client, mockLocalIndexExporter);
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        // Verify exporter is set to NONE
        assertEquals(SinkType.DEBUG, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromNoneToLocalIndex() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.NONE;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(null);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(null);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.LOCAL_INDEX);
        // Verify new local index exporter setup
        // 2 times, one for initialization, one for the above method call
        verify(queryInsightsExporterFactory, times(2)).createLocalIndexExporter(
            eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID),
            anyString(),
            anyString()
        );
        verify(queryInsightsReaderFactory, times(2)).createLocalIndexReader(
            eq(TopQueriesService.TOP_QUERIES_READER_ID),
            anyString(),
            eq(namedXContentRegistry)
        );
        verify(queryInsightsExporterFactory, times(0)).closeExporter(any());
        verify(queryInsightsReaderFactory, times(0)).closeReader(any());
        verify(queryInsightsServiceSpy, times(0)).deleteAllTopNIndices(any(), any());
        assertEquals(SinkType.LOCAL_INDEX, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromNoneToDebug() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.NONE;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(null);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(null);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify new local index exporter setup
        verify(queryInsightsExporterFactory, times(1)).createDebugExporter(eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID));
        verify(queryInsightsServiceSpy, times(0)).deleteAllTopNIndices(any(), any());
        verify(queryInsightsExporterFactory, times(0)).closeExporter(any());
        verify(queryInsightsReaderFactory, times(0)).closeReader(any());
        assertEquals(SinkType.DEBUG, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromDebugToLocalIndex() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.DEBUG;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockDebugExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.LOCAL_INDEX);
        // Verify new local index exporter setup
        // 2 times, one for initialization, one for the above method call
        verify(queryInsightsExporterFactory, times(2)).createLocalIndexExporter(
            eq(TopQueriesService.TOP_QUERIES_EXPORTER_ID),
            anyString(),
            anyString()
        );
        verify(queryInsightsReaderFactory, times(2)).createLocalIndexReader(
            eq(TopQueriesService.TOP_QUERIES_READER_ID),
            anyString(),
            eq(namedXContentRegistry)
        );
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockDebugExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        verify(queryInsightsServiceSpy, times(0)).deleteAllTopNIndices(any(), any());
        assertEquals(SinkType.LOCAL_INDEX, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_SwitchFromDebugToNone() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.DEBUG;
        // Mock current exporter and reader
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockDebugExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.NONE);
        // Verify new local index exporter setup
        // 2 times, one for initialization, one for the above method call
        verify(queryInsightsServiceSpy, times(0)).deleteAllTopNIndices(client, mockLocalIndexExporter);
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockDebugExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(mockReader);
        assertEquals(SinkType.NONE, queryInsightsServiceSpy.sinkType);
    }

    public void testSetExporterAndReaderType_CloseWithException() throws IOException {
        queryInsightsServiceSpy.sinkType = SinkType.LOCAL_INDEX;
        doNothing().when(queryInsightsServiceSpy).deleteAllTopNIndices(any(), any());
        // Mock current exporter that throws an exception when closing
        when(queryInsightsExporterFactory.getExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID)).thenReturn(mockLocalIndexExporter);
        when(queryInsightsReaderFactory.getReader(TopQueriesService.TOP_QUERIES_READER_ID)).thenReturn(mockReader);
        doThrow(new IOException("Exporter close error")).when(queryInsightsExporterFactory).closeExporter(mockLocalIndexExporter);
        doThrow(new IOException("Reader close error")).when(queryInsightsReaderFactory).closeReader(mockReader);
        // Execute method
        queryInsightsServiceSpy.setExporterAndReaderType(SinkType.DEBUG);
        // Verify exception handling
        verify(queryInsightsExporterFactory, times(1)).closeExporter(mockLocalIndexExporter);
        verify(queryInsightsReaderFactory, times(1)).closeReader(any());
        // Ensure new exporter is still created
        verify(queryInsightsExporterFactory, times(1)).createDebugExporter(TopQueriesService.TOP_QUERIES_EXPORTER_ID);
    }

    public void testGetInitialDelay() {
        //// First Test Case (Normal Case) ////
        Instant instantNormal = Instant.parse("2025-03-26T10:15:30Z");
        Instant startOfNextDayNormal = instantNormal.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS);
        final long expectedDelayNormal = Duration.between(instantNormal, startOfNextDayNormal).toMillis();
        assertEquals(expectedDelayNormal, getInitialDelay(instantNormal));

        //// Second Test Case (Edge Case: Midnight UTC) ////
        Instant instantEdge = Instant.parse("2025-03-26T00:00:00Z");
        Instant startOfNextDayEdge = instantEdge.plus(1, ChronoUnit.DAYS);
        final long expectedDelayEdge = Duration.between(instantEdge, startOfNextDayEdge).toMillis();
        assertEquals(expectedDelayEdge, getInitialDelay(instantEdge));
    }

    public void testGetInitialDelayComprehensive() {
        // Test various times throughout the day
        Instant morning = Instant.parse("2025-01-15T08:30:45Z");
        long morningDelay = getInitialDelay(morning);
        assertTrue("Morning delay should be positive", morningDelay > 0);
        assertTrue("Morning delay should be less than 24 hours", morningDelay < Duration.ofDays(1).toMillis());

        Instant afternoon = Instant.parse("2025-01-15T14:22:10Z");
        long afternoonDelay = getInitialDelay(afternoon);
        assertTrue("Afternoon delay should be positive", afternoonDelay > 0);
        assertTrue("Afternoon delay should be less than morning delay", afternoonDelay < morningDelay);

        Instant lateNight = Instant.parse("2025-01-15T23:59:59Z");
        long lateNightDelay = getInitialDelay(lateNight);
        assertEquals("Late night delay should be 1 second", 1000, lateNightDelay);

        // Test edge case: exactly midnight
        Instant exactMidnight = Instant.parse("2025-01-15T00:00:00Z");
        long midnightDelay = getInitialDelay(exactMidnight);
        assertEquals("Midnight delay should be exactly 24 hours", Duration.ofDays(1).toMillis(), midnightDelay);
    }

    public void testSchedulerSetupAndLifecycle() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.getFirst();

        // Test scheduler is not set up initially
        assertNull("Scheduler should not be set up initially", updatedQueryInsightsService.deleteIndicesScheduledFuture);
        assertNull("Scheduled futures should not be set up initially", updatedQueryInsightsService.scheduledFutures);

        // Start the service
        updatedQueryInsightsService.doStart();

        // Verify scheduler is set up
        assertNotNull("Delete indices scheduler should be set up", updatedQueryInsightsService.deleteIndicesScheduledFuture);
        assertFalse("Delete indices scheduler should not be cancelled", updatedQueryInsightsService.deleteIndicesScheduledFuture.isCancelled());
        assertNotNull("Scheduled futures should be set up", updatedQueryInsightsService.scheduledFutures);
        assertEquals("Should have one scheduled future for drain records", 1, updatedQueryInsightsService.scheduledFutures.size());

        // Stop the service
        updatedQueryInsightsService.doStop();

        // Verify scheduler is cancelled
        assertTrue("Delete indices scheduler should be cancelled", updatedQueryInsightsService.deleteIndicesScheduledFuture.isCancelled());
        assertTrue("Drain records scheduler should be cancelled", updatedQueryInsightsService.scheduledFutures.get(0).isCancelled());

        cleanupTestServices(updatedService);
    }

    public void testSchedulerWithDisabledFeatures() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.getFirst();

        // Disable all features
        updatedQueryInsightsService.enableCollection(MetricType.LATENCY, false);
        updatedQueryInsightsService.enableCollection(MetricType.CPU, false);
        updatedQueryInsightsService.enableCollection(MetricType.MEMORY, false);

        // Start the service
        updatedQueryInsightsService.doStart();

        // Verify scheduler is not set up when features are disabled
        assertNull("Delete indices scheduler should not be set up when features disabled",
                   updatedQueryInsightsService.deleteIndicesScheduledFuture);
        assertNull("Scheduled futures should not be set up when features disabled",
                   updatedQueryInsightsService.scheduledFutures);

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithRetentionPeriod3Days() throws IOException, InterruptedException {
        testDeleteExpiredIndicesWithRetentionPeriod(3, 5, 2); // 5 indices, expect 2 deleted
    }

    public void testDeleteExpiredTopNIndicesWithRetentionPeriod1Day() throws IOException, InterruptedException {
        testDeleteExpiredIndicesWithRetentionPeriod(1, 5, 4); // 5 indices, expect 4 deleted
    }

    public void testDeleteExpiredTopNIndicesWithRetentionPeriod10Days() throws IOException, InterruptedException {
        testDeleteExpiredIndicesWithRetentionPeriod(10, 5, 0); // 5 indices, expect 0 deleted
    }

    public void testDeleteExpiredTopNIndicesWithNoExpiredIndices() throws IOException, InterruptedException {
        // Create indices that are all recent (within retention period)
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 1; i <= 3; i++) {
            LocalDate date = LocalDate.now().minusDays(i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set retention period to 7 days (default)
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(7);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

        updatedQueryInsightsService.deleteExpiredTopNIndices();

        assertTrue("Cluster state request should be made", latch.await(5, TimeUnit.SECONDS));
        // Verify no indices are deleted since all are within retention period
        verify(client, times(1)).admin(); // Only one call to get cluster state
        verify(adminClient, times(0)).indices(); // No deletion calls

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithEmptyCluster() throws IOException, InterruptedException {
        // Test with empty cluster (no indices)
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

        updatedQueryInsightsService.deleteExpiredTopNIndices();

        assertTrue("Cluster state request should be made", latch.await(5, TimeUnit.SECONDS));
        // Verify no deletion attempts are made
        verify(client, times(1)).admin(); // Only one call to get cluster state
        verify(adminClient, times(0)).indices(); // No deletion calls

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithMixedIndices() throws IOException, InterruptedException {
        // Create a mix of top queries indices and regular indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        // Add 3 expired top queries indices
        for (int i = 8; i <= 10; i++) {
            LocalDate date = LocalDate.now().minusDays(i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        // Add 2 recent top queries indices (should not be deleted)
        for (int i = 1; i <= 2; i++) {
            LocalDate date = LocalDate.now().minusDays(i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        // Add regular indices (should not be deleted regardless of age)
        for (int i = 1; i <= 3; i++) {
            String indexName = "regular_index_" + i;
            long creationTime = Instant.now().minus(20, ChronoUnit.DAYS).toEpochMilli(); // Very old

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .put(SETTING_CREATION_DATE, creationTime))
                .build();
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set retention period to 7 days
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(7);

        final int expectedIndicesDeleted = 3; // Only the 3 expired top queries indices
        CountDownLatch latch = new CountDownLatch(expectedIndicesDeleted);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(indicesAdminClient).delete(any(), any());

        updatedQueryInsightsService.deleteExpiredTopNIndices();

        assertTrue("Expected number of indices should be deleted", latch.await(10, TimeUnit.SECONDS));
        verify(client, times(expectedIndicesDeleted + 1)).admin(); // +1 for cluster state request
        verify(adminClient, times(expectedIndicesDeleted)).indices();
        verify(indicesAdminClient, times(expectedIndicesDeleted)).delete(any(), any());

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesErrorHandling() throws IOException, InterruptedException {
        // Create some expired indices
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (int i = 8; i <= 10; i++) {
            LocalDate date = LocalDate.now().minusDays(i);
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Mock cluster state request to throw an exception
        doAnswer(invocation -> {
            ActionListener<ClusterStateResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("Cluster state request failed"));
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

        // This should not throw an exception - errors should be handled gracefully
        try {
            updatedQueryInsightsService.deleteExpiredTopNIndices();
            // Give some time for async operation to complete
            Thread.sleep(100);
        } catch (Exception e) {
            fail("deleteExpiredTopNIndices should handle errors gracefully, but threw: " + e.getMessage());
        }

        // Verify cluster state was requested but no deletions occurred due to error
        verify(client, times(1)).admin();
        verify(adminClient, times(0)).indices(); // No deletion calls due to error

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithNullExporter() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Reset the mock to clear any previous interactions from setup
        reset(client);
        // Re-setup the mock after reset to avoid NullPointerException
        when(client.admin()).thenReturn(adminClient);

        // Set exporter type to DEBUG to avoid creating a LocalIndexExporter
        // This simulates the case where no LocalIndexExporter is available
        // Note: This will trigger deleteAllTopNIndices during the transition from LOCAL_INDEX to DEBUG
        updatedQueryInsightsService.setExporterAndReaderType(SinkType.DEBUG);

        // Reset the mock again to clear the admin call from setExporterAndReaderType
        reset(client);

        // This should not throw an exception and should not make any cluster calls
        try {
            updatedQueryInsightsService.deleteExpiredTopNIndices();
        } catch (Exception e) {
            fail("deleteExpiredTopNIndices should handle non-LocalIndexExporter gracefully, but threw: " + e.getMessage());
        }

        // Verify no cluster state requests are made when exporter is not LocalIndexExporter
        verify(client, times(0)).admin();

        cleanupTestServices(updatedService);
    }

    public void testSetExporterDeleteAfterAndDelete() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Get the local index exporter
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);

        // Verify initial delete after value
        assertEquals("Initial delete after should be default",
                     QueryInsightsSettings.DEFAULT_DELETE_AFTER_VALUE, localExporter.getDeleteAfter());

        // Test setting new delete after value
        updatedQueryInsightsService.setExporterDeleteAfterAndDelete(14);

        // Verify the value was updated
        assertEquals("Delete after should be updated", 14, localExporter.getDeleteAfter());

        cleanupTestServices(updatedService);
    }

    public void testSchedulerTimingConfiguration() throws IOException {
        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(Map.of());
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        updatedQueryInsightsService.doStart();

        // Verify scheduler is configured with correct timing
        assertNotNull("Delete indices scheduler should be set up", updatedQueryInsightsService.deleteIndicesScheduledFuture);

        // The scheduler should be configured to run daily (24 hours = 86400000 ms)
        // We can't directly access the period, but we can verify the scheduler was created
        assertFalse("Scheduler should not be cancelled", updatedQueryInsightsService.deleteIndicesScheduledFuture.isCancelled());

        // Test that initial delay is calculated correctly for current time
        long currentInitialDelay = getInitialDelay(Instant.now());
        assertTrue("Initial delay should be positive", currentInitialDelay > 0);
        assertTrue("Initial delay should be less than 24 hours", currentInitialDelay <= Duration.ofDays(1).toMillis());

        cleanupTestServices(updatedService);
    }

    public void testExpirationCalculationLogic() {
        // Test the expiration calculation logic used in deleteExpiredTopNIndices

        // Mock current time for consistent testing
        Instant fixedNow = Instant.parse("2025-01-15T12:30:45Z");

        // Calculate start of today UTC (should be 2025-01-15T00:00:00Z)
        long startOfTodayUtcMillis = fixedNow.truncatedTo(ChronoUnit.DAYS).toEpochMilli();
        assertEquals("Start of today should be midnight UTC",
                     Instant.parse("2025-01-15T00:00:00Z").toEpochMilli(), startOfTodayUtcMillis);

        // Test with different retention periods
        int retentionDays = 7;
        long expirationMillis = startOfTodayUtcMillis - TimeUnit.DAYS.toMillis(retentionDays);
        assertEquals("Expiration should be 7 days before start of today",
                     Instant.parse("2025-01-08T00:00:00Z").toEpochMilli(), expirationMillis);

        // Test edge case: retention period of 1 day
        retentionDays = 1;
        expirationMillis = startOfTodayUtcMillis - TimeUnit.DAYS.toMillis(retentionDays);
        assertEquals("Expiration should be 1 day before start of today",
                     Instant.parse("2025-01-14T00:00:00Z").toEpochMilli(), expirationMillis);

        // Test maximum retention period
        retentionDays = 180;
        expirationMillis = startOfTodayUtcMillis - TimeUnit.DAYS.toMillis(retentionDays);
        assertEquals("Expiration should be 180 days before start of today",
                     Instant.parse("2024-07-19T00:00:00Z").toEpochMilli(), expirationMillis);
    }

    // Util functions
    private void testDeleteExpiredIndicesWithRetentionPeriod(int retentionDays, int totalIndices, int expectedDeleted)
            throws IOException, InterruptedException {
        // Create indices with different ages - some expired, some not
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        // Create expired indices (older than retention period)
        for (int i = 0; i < expectedDeleted; i++) {
            LocalDate date = LocalDate.now().minusDays(retentionDays + i + 1); // Definitely expired
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(retentionDays + i + 1, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        // Create non-expired indices (within retention period)
        for (int i = 0; i < (totalIndices - expectedDeleted); i++) {
            LocalDate date = LocalDate.now().minusDays(i + 1); // Within retention period
            String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
            long creationTime = Instant.now().minus(i + 1, ChronoUnit.DAYS).toEpochMilli();

            IndexMetadata indexMetadata = createTopQueriesIndexMetadata(indexName, creationTime);
            indexMetadataMap.put(indexName, indexMetadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set the retention period
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(retentionDays);

        if (expectedDeleted > 0) {
            CountDownLatch latch = new CountDownLatch(expectedDeleted);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(indicesAdminClient).delete(any(), any());

            updatedQueryInsightsService.deleteExpiredTopNIndices();

            assertTrue("Expected number of indices should be deleted", latch.await(10, TimeUnit.SECONDS));
            // Verify that deletion operations were called the expected number of times
            verify(indicesAdminClient, times(expectedDeleted)).delete(any(), any());
        } else {
            CountDownLatch latch = new CountDownLatch(1);
            doAnswer(invocation -> {
                latch.countDown();
                return null;
            }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

            updatedQueryInsightsService.deleteExpiredTopNIndices();

            assertTrue("Cluster state request should be made", latch.await(5, TimeUnit.SECONDS));
            // Verify no deletion operations were called
            verify(indicesAdminClient, times(0)).delete(any(), any());
        }

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithInvalidIndexNames() throws IOException, InterruptedException {
        // Test indices with invalid names that should not be deleted
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        // Create indices with invalid names but valid metadata
        String[] invalidNames = {
            "top_queries-invalid-date-12345",  // Invalid date format
            "top_queries-2024.01.01",         // Missing hash part
            "top_queries-2024.01.01-abc12",   // Non-numeric hash
            "top_queries-2024.01.01-123456",  // Wrong hash length
            "wrong_prefix-2024.01.01-12345",  // Wrong prefix
            "top_queries-2024.01.01-12345-extra" // Too many parts
        };

        for (String invalidName : invalidNames) {
            long creationTime = Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli();
            IndexMetadata metadata = createTopQueriesIndexMetadata(invalidName, creationTime);
            indexMetadataMap.put(invalidName, metadata);
        }

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set the retention period to ensure indices would be expired if they were valid
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(7);

        // Execute deletion - should not delete any indices due to invalid names
        updatedQueryInsightsService.deleteExpiredTopNIndices();

        // Give some time for async operations to complete
        Thread.sleep(100);

        // Verify no deletion operations were called for invalid index names
        verify(indicesAdminClient, times(0)).delete(any(), any());

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithInvalidMetadata() throws IOException, InterruptedException {
        // Test indices with valid names but invalid metadata
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        LocalDate date = LocalDate.now().minusDays(10);
        String validIndexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
        long creationTime = Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli();

        // Create index with missing _meta field
        IndexMetadata invalidMetadata = IndexMetadata.builder(validIndexName)
            .settings(Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put(SETTING_CREATION_DATE, creationTime))
            .putMapping(new MappingMetadata("_doc", Map.of("properties", Map.of("field", Map.of("type", "text")))))
            .build();
        indexMetadataMap.put(validIndexName, invalidMetadata);

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set the retention period to ensure indices would be expired if they were valid
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(7);

        // Execute deletion - should not delete any indices due to invalid metadata
        updatedQueryInsightsService.deleteExpiredTopNIndices();

        // Give some time for async operations to complete
        Thread.sleep(100);

        // Verify no deletion operations were called for invalid metadata
        verify(indicesAdminClient, times(0)).delete(any(), any());

        cleanupTestServices(updatedService);
    }

    public void testDeleteExpiredTopNIndicesWithWrongMetaTagValue() throws IOException, InterruptedException {
        // Test indices with correct structure but wrong meta tag value
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        LocalDate date = LocalDate.now().minusDays(10);
        String indexName = "top_queries-" + date.format(format) + "-" + generateLocalIndexDateHash(date);
        long creationTime = Instant.now().minus(10, ChronoUnit.DAYS).toEpochMilli();

        // Create index with wrong meta tag value
        IndexMetadata wrongTagMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put(SETTING_CREATION_DATE, creationTime))
            .putMapping(new MappingMetadata("_doc",
                Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, "wrong_tag_value"))))
            .build();
        indexMetadataMap.put(indexName, wrongTagMetadata);

        List<AbstractLifecycleComponent> updatedService = createQueryInsightsServiceWithIndexState(indexMetadataMap);
        QueryInsightsService updatedQueryInsightsService = (QueryInsightsService) updatedService.get(0);

        // Set the retention period to ensure indices would be expired if they were valid
        LocalIndexExporter localExporter = (LocalIndexExporter) updatedQueryInsightsService
            .queryInsightsExporterFactory.getExporter(TOP_QUERIES_EXPORTER_ID);
        localExporter.setDeleteAfter(7);

        // Execute deletion - should not delete any indices due to wrong meta tag
        updatedQueryInsightsService.deleteExpiredTopNIndices();

        // Give some time for async operations to complete
        Thread.sleep(100);

        // Verify no deletion operations were called for wrong meta tag
        verify(indicesAdminClient, times(0)).delete(any(), any());

        cleanupTestServices(updatedService);
    }



    private IndexMetadata createTopQueriesIndexMetadata(String indexName, long creationTime) {
        return IndexMetadata.builder(indexName)
            .settings(Settings.builder()
                .put("index.version.created", Version.CURRENT.id)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put(SETTING_CREATION_DATE, creationTime))
            .putMapping(new MappingMetadata("_doc",
                Map.of("_meta", Map.of(QUERY_INSIGHTS_INDEX_TAG_NAME, TOP_QUERIES_INDEX_TAG_VALUE))))
            .build();
    }

    private List<AbstractLifecycleComponent> createQueryInsightsServiceWithIndexState(Map<String, IndexMetadata> indexMetadataMap) {
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryInsightsTestUtils.registerAllQueryInsightsSettings(clusterSettings);
        // Create a mock cluster state with expired indices
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary("", true, 1 + randomInt(3), randomInt(2));
        RoutingTable.Builder routingTable = RoutingTable.builder(state.routingTable());
        indexMetadataMap.forEach((indexName, indexMetadata) -> { routingTable.addAsRecovery(indexMetadata); });
        // Update the cluster state with the new indices
        ClusterState updatedState = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).indices(indexMetadataMap).build())
            .routingTable(routingTable.build())
            .build();
        // Create a new cluster service with the updated state
        ClusterService updatedClusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            state.getNodes().getLocalNode(),
            clusterSettings
        );
        ClusterServiceUtils.setState(updatedClusterService, updatedState);

        ClusterStateResponse mockClusterStateResponse = mock(ClusterStateResponse.class);
        when(mockClusterStateResponse.getState()).thenReturn(updatedState);

        doAnswer(invocation -> {
            ActionListener<ClusterStateResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(mockClusterStateResponse);
            return null;
        }).when(clusterAdminClient).state(any(ClusterStateRequest.class), any(ActionListener.class));

        // Initialize the QueryInsightsService with the new cluster service
        QueryInsightsService updatedQueryInsightsService = new QueryInsightsService(
            updatedClusterService,
            threadPool,
            client,
            NoopMetricsRegistry.INSTANCE,
            namedXContentRegistry,
            new QueryInsightsExporterFactory(client, clusterService),
            new QueryInsightsReaderFactory(client)
        );
        updatedQueryInsightsService.enableCollection(MetricType.LATENCY, true);
        updatedQueryInsightsService.enableCollection(MetricType.CPU, true);
        updatedQueryInsightsService.enableCollection(MetricType.MEMORY, true);
        updatedQueryInsightsService.setQueryShapeGenerator(new QueryShapeGenerator(updatedClusterService));
        // Create a local index exporter with a retention period of 7 days
        updatedQueryInsightsService.queryInsightsExporterFactory.createLocalIndexExporter(TOP_QUERIES_EXPORTER_ID, "YYYY.MM.dd", "");
        return List.of(updatedQueryInsightsService, updatedClusterService);
    }
}
