/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.QueryBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Local index reader for reading query insights data from local OpenSearch indices.
 */
public final class LocalIndexReader implements QueryInsightsReader {
    /**
     * Logger of the local index reader
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private DateTimeFormatter indexPattern;
    private NamedXContentRegistry namedXContentRegistry;

    /**
     * Constructor of LocalIndexReader
     *
     * @param client OS client
     * @param indexPattern the pattern of index to read from
     */
    public LocalIndexReader(final Client client, final DateTimeFormatter indexPattern, NamedXContentRegistry namedXContentRegistry) {
        this.indexPattern = indexPattern;
        this.client = client;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Getter of indexPattern
     *
     * @return indexPattern
     */
    public DateTimeFormatter getIndexPattern() {
        return indexPattern;
    }

    /**
     * Setter of indexPattern
     *
     * @param indexPattern index pattern
     * @return the current LocalIndexReader
     */
    public LocalIndexReader setIndexPattern(DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
        return this;
    }

    /**
     * Export a list of SearchQueryRecord from local index
     *
     *
     */
    @Override
    public List<SearchQueryRecord> read(final String from, final String to) {
        List<SearchQueryRecord> records = new ArrayList<>();
        try {
            final DateTime start = new DateTime(Long.valueOf(from), DateTimeZone.UTC);
            final DateTime end = new DateTime(Long.valueOf(to), DateTimeZone.UTC);
            DateTime curr = start;
            while (curr != end) {
                String index = getDateTimeFromFormat(curr);
                SearchRequest searchRequest = new SearchRequest(index);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.rangeQuery("timestamp").from(from).to(to));
                searchRequest.source(searchSourceBuilder);
                SearchResponse searchResponse = client.search(searchRequest).actionGet();
                for (SearchHit hit : searchResponse.getHits()) {
                    SearchQueryRecord record = SearchQueryRecord.getRecord(hit, namedXContentRegistry);
                    records.add(record);
                }
                curr = curr.plusDays(1);

            }
        } catch (Exception e) {
            logger.error("Failed to read query insights data: ", e);
        }
        return records;
    }

    /**
     * Close the reader sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexReader..");
    }

    private String getDateTimeFromFormat(DateTime current) {
        return indexPattern.print(current);
    }
}
