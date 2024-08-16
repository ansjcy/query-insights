/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.*;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

/**
 * SearchQueryRecord represents a minimal atomic record stored in the Query Insight Framework,
 * which contains extensive information related to a search query.
 */
public class SearchQueryRecord implements ToXContentObject, Writeable {
    private static final Logger log = LogManager.getLogger(SearchQueryRecord.class);
    private final long timestamp;
    private final Map<MetricType, Number> measurements;
    private final Map<Attribute, Object> attributes;
    public static final String TIMESTAMP = "timestamp";
    public static final String LATENCY = "latency";
    public static final String CPU = "cpu";
    public static final String MEMORY = "memory";
    public static final String SEARCH_TYPE = "search_type";
    public static final String SOURCE = "source";
    public static final String TOTAL_SHARDS = "total_shards";
    public static final String INDICES = "indices";
    public static final String PHASE_LATENCY_MAP = "phase_latency_map";
    public static final String NODE_ID = "node_id";
    public static final String TASK_RESOURCE_USAGES = "task_resource_usages";
    public static final String LABELS = "labels";

    /**
     * Constructor of SearchQueryRecord
     *
     * @param in the StreamInput to read the SearchQueryRecord from
     * @throws IOException IOException
     * @throws ClassCastException ClassCastException
     */
    public SearchQueryRecord(final StreamInput in) throws IOException, ClassCastException {
        this.timestamp = in.readLong();
        measurements = new HashMap<>();
        in.readMap(MetricType::readFromStream, StreamInput::readGenericValue)
            .forEach(((metricType, o) -> measurements.put(metricType, metricType.parseValue(o))));
        this.attributes = Attribute.readAttributeMap(in);
    }

    /**
     * Constructor of SearchQueryRecord
     *
     * @param timestamp The timestamp of the query.
     * @param measurements A list of Measurement associated with this query
     * @param attributes A list of Attributes associated with this query
     */
    public SearchQueryRecord(final long timestamp, Map<MetricType, Number> measurements, final Map<Attribute, Object> attributes) {
        if (measurements == null) {
            throw new IllegalArgumentException("Measurements cannot be null");
        }
        this.measurements = measurements;
        this.attributes = attributes;
        this.timestamp = timestamp;
    }

    public static SearchQueryRecord getRecord(SearchHit hit, NamedXContentRegistry namedXContentRegistry) throws IOException {
        long timestamp = 0L;
        Map<MetricType, Number> measurements = new HashMap<>();
        Map<Attribute, Object> attributes = new HashMap<>();
        XContentParser parser = XContentType.JSON.xContent().createParser(
//            NamedXContentRegistry.EMPTY,
            namedXContentRegistry,
            LoggingDeprecationHandler.INSTANCE,
            hit.getSourceAsString()
        );
        parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case TIMESTAMP:
                    timestamp = parser.longValue();
                    break;
                case LATENCY:
                case CPU:
                case MEMORY:
                    MetricType metric = MetricType.fromString(fieldName);
                    measurements.put(metric, metric.parseValue(parser.numberValue()));
                    break;
                case SEARCH_TYPE:
                    attributes.put(Attribute.SEARCH_TYPE, parser.text());
                    break;
                case SOURCE:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    attributes.put(Attribute.SOURCE, SearchSourceBuilder.fromXContent(parser, false));
                    break;
                case TOTAL_SHARDS:
                    attributes.put(Attribute.TOTAL_SHARDS, parser.intValue());
                    break;
                case INDICES:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    List<String> indices = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        indices.add(parser.text());
                    }
                    attributes.put(Attribute.INDICES, indices.toArray());
                    break;
                case PHASE_LATENCY_MAP:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    Map<String, Long> phaseLatencyMap = new HashMap<>();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String phase = parser.currentName();
                        parser.nextToken();
                        phaseLatencyMap.put(phase, parser.longValue());
                    }
                    attributes.put(Attribute.PHASE_LATENCY_MAP, phaseLatencyMap);
                    break;
                case NODE_ID:
                    attributes.put(Attribute.NODE_ID, parser.text());
                    break;
                case TASK_RESOURCE_USAGES:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    List<TaskResourceInfo> tasksResourceUsages = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String action = "";
                        long taskId = 0L;
                        long parentTaskId = 0L;
                        String nodeId = "";
                        TaskResourceUsage taskRU = new TaskResourceUsage(0L, 0L);
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            String usageFields = parser.currentName();
                            parser.nextToken();
                            switch (usageFields) {
                                case "action":
                                    action = parser.text();
                                    break;
                                case "taskId":
                                    taskId = parser.longValue();
                                    break;
                                case "parentTaskId":
                                    parentTaskId = parser.longValue();
                                    break;
                                case "nodeId":
                                    nodeId = parser.text();
                                    break;
                                case "taskResourceUsage":
                                    taskRU = TaskResourceUsage.fromXContent(parser);
                                    break;
                                default:
                                    break;
                            }
                        }
                        TaskResourceInfo resourceInfo = new TaskResourceInfo(action, taskId, parentTaskId, nodeId, taskRU);
                        tasksResourceUsages.add(resourceInfo);
                    }
                    attributes.put(Attribute.TASK_RESOURCE_USAGES, tasksResourceUsages);
                    break;
                case LABELS:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    Map<String, Object> labels = new HashMap<>();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        parser.nextToken();
                        labels.put(Task.X_OPAQUE_ID, parser.text());
                    }
                    attributes.put(Attribute.LABELS, labels);
                    break;
                default:
                    break;
            }
        }
        return new SearchQueryRecord(timestamp, measurements, attributes);
    }

    /**
     * Returns the observation time of the metric.
     *
     * @return the observation time in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the measurement associated with the specified name.
     *
     * @param name the name of the measurement
     * @return the measurement object, or null if not found
     */
    public Number getMeasurement(final MetricType name) {
        return measurements.get(name);
    }

    /**
     * Returns a map of all the measurements associated with the metric.
     *
     * @return a map of measurement names to measurement objects
     */
    public Map<MetricType, Number> getMeasurements() {
        return measurements;
    }

    /**
     * Returns a map of the attributes associated with the metric.
     *
     * @return a map of attribute keys to attribute values
     */
    public Map<Attribute, Object> getAttributes() {
        return attributes;
    }

    /**
     * Add an attribute to this record
     *
     * @param attribute attribute to add
     * @param value the value associated with the attribute
     */
    public void addAttribute(final Attribute attribute, final Object value) {
        attributes.put(attribute, value);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("timestamp", timestamp);
        for (Map.Entry<Attribute, Object> entry : attributes.entrySet()) {
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        for (Map.Entry<MetricType, Number> entry : measurements.entrySet()) {
            builder.field(entry.getKey().toString(), entry.getValue());
        }
        return builder.endObject();
    }

    /**
     * Write a SearchQueryRecord to a StreamOutput
     *
     * @param out the StreamOutput to write
     * @throws IOException IOException
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeMap(measurements, (stream, metricType) -> MetricType.writeTo(out, metricType), StreamOutput::writeGenericValue);
        out.writeMap(
            attributes,
            (stream, attribute) -> Attribute.writeTo(out, attribute),
            (stream, attributeValue) -> Attribute.writeValueTo(out, attributeValue)
        );
    }

    /**
     * Compare two SearchQueryRecord, based on the given MetricType
     *
     * @param a the first SearchQueryRecord to compare
     * @param b the second SearchQueryRecord to compare
     * @param metricType the MetricType to compare on
     * @return 0 if the first SearchQueryRecord is numerically equal to the second SearchQueryRecord;
     *        -1 if the first SearchQueryRecord is numerically less than the second SearchQueryRecord;
     *         1 if the first SearchQueryRecord is numerically greater than the second SearchQueryRecord.
     */
    public static int compare(final SearchQueryRecord a, final SearchQueryRecord b, final MetricType metricType) {
        return metricType.compare(a.getMeasurement(metricType), b.getMeasurement(metricType));
    }

    /**
     * Check if a SearchQueryRecord is deep equal to another record
     *
     * @param o the other SearchQueryRecord record
     * @return true if two records are deep equal, false otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SearchQueryRecord)) {
            return false;
        }
        final SearchQueryRecord other = (SearchQueryRecord) o;
        return timestamp == other.getTimestamp()
            && measurements.equals(other.getMeasurements())
            && attributes.size() == other.getAttributes().size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, measurements, attributes);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
