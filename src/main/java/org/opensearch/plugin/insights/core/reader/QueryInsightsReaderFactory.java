/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.reader;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;

/**
 * Factory class for validating and creating Readers based on provided settings
 */
public class QueryInsightsReaderFactory {
    /**
     * Logger of the query insights Reader factory
     */
    private final Logger logger = LogManager.getLogger();
    final private Client client;
    final private Map<String, QueryInsightsReader> readers;

    /**
     * Constructor of QueryInsightsReaderFactory
     *
     * @param client OS client
     */
    public QueryInsightsReaderFactory(final Client client) {
        this.client = client;
        this.readers = new HashMap<>();
    }

    /**
     * Create a Reader based on provided parameters
     *
     * @param indexPattern the index pattern if creating an index Reader
     * @param namedXContentRegistry for parsing purposes
     * @return QueryInsightsReader the created Reader
     */
    public QueryInsightsReader createReader(String id, String indexPattern, NamedXContentRegistry namedXContentRegistry) {
        QueryInsightsReader reader = new LocalIndexReader(
            client,
            DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT),
            namedXContentRegistry,
            id
        );
        this.readers.put(id, reader);
        return reader;
    }

    /**
     * Update a reader based on provided parameters
     *
     * @param reader The reader to update
     * @param indexPattern the index pattern if creating an index reader
     * @return QueryInsightsReader the updated reader sink
     */
    public QueryInsightsReader updateReader(QueryInsightsReader reader, String indexPattern) {
        if (reader.getClass() == LocalIndexReader.class) {
            ((LocalIndexReader) reader).setIndexPattern(DateTimeFormatter.ofPattern(indexPattern, Locale.ROOT));
        }
        return reader;
    }

    /**
     * Get a Reader by id
     * @param id The id of the reader
     * @return QueryInsightsReader the Reader
     */
    public QueryInsightsReader getReader(String id) {
        return this.readers.get(id);
    }

    /**
     * Close a Reader
     *
     * @param Reader the Reader to close
     */
    public void closeReader(QueryInsightsReader Reader) throws IOException {
        if (Reader != null) {
            Reader.close();
            this.readers.remove(Reader.getId());
        }
    }

    /**
     * Close all Readers
     *
     */
    public void closeAllReaders() {
        for (QueryInsightsReader reader : readers.values()) {
            try {
                closeReader(reader);
            } catch (IOException e) {
                logger.error("Fail to close query insights Reader, error: ", e);
            }
        }
    }
}
