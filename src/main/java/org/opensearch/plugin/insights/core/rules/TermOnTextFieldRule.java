/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.rules;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.plugin.insights.core.service.recommendations.QueryContext;
import org.opensearch.plugin.insights.rules.model.recommendations.Action;
import org.opensearch.plugin.insights.rules.model.recommendations.Direction;
import org.opensearch.plugin.insights.rules.model.recommendations.ImpactVector;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.rules.model.recommendations.RecommendationType;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Rule that detects term/terms queries on text fields
 *
 * This is a correctness anti-pattern because text fields are analyzed (tokenized, lowercased),
 * while term queries search for exact matches. This usually results in zero or incomplete results.
 *
 * Example:
 * - Index contains: {"title": "OpenSearch Query"} (stored as tokens: ["opensearch", "query"])
 * - Query: {"term": {"title": "OpenSearch"}} (searches for exact "OpenSearch")
 * - Result: 0 documents (because "OpenSearch" != "opensearch")
 */
public class TermOnTextFieldRule implements RecommendationRule {
    private static final Logger log = LogManager.getLogger(TermOnTextFieldRule.class);

    private static final String RULE_ID = "term-on-text-field";
    private static final String RULE_NAME = "Term Query on Text Field";
    private static final String RULE_DESCRIPTION = "Detects term/terms queries on analyzed text fields";
    private static final double BASE_CONFIDENCE = 0.98;

    @Override
    public String getId() {
        return RULE_ID;
    }

    @Override
    public String getName() {
        return RULE_NAME;
    }

    @Override
    public String getDescription() {
        return RULE_DESCRIPTION;
    }

    @Override
    public int getPriority() {
        return 10; // High priority - this is a critical correctness issue
    }

    @Override
    public boolean matches(QueryContext context) {
        try {
            // Check for term queries on text fields
            List<TermQueryBuilder> termQueries = context.extractTermQueries();
            log.info("TermOnTextFieldRule: Found {} term queries", termQueries.size());
            for (TermQueryBuilder termQuery : termQueries) {
                String fieldName = termQuery.fieldName();
                log.info("TermOnTextFieldRule: Checking term query on field: {}", fieldName);
                if (fieldName != null && isTextFieldType(context, fieldName)) {
                    log.info("TermOnTextFieldRule: MATCH - term query on text field: {}", fieldName);
                    return true;
                }
            }

            // Check for terms queries on text fields
            List<TermsQueryBuilder> termsQueries = context.extractTermsQueries();
            log.info("TermOnTextFieldRule: Found {} terms queries", termsQueries.size());
            for (TermsQueryBuilder termsQuery : termsQueries) {
                String fieldName = termsQuery.fieldName();
                log.info("TermOnTextFieldRule: Checking terms query on field: {}", fieldName);
                if (fieldName != null && isTextFieldType(context, fieldName)) {
                    log.info("TermOnTextFieldRule: MATCH - terms query on text field: {}", fieldName);
                    return true;
                }
            }

            log.info("TermOnTextFieldRule: No match found for query: {}", context.getRecord().getId());
            return false;
        } catch (Exception e) {
            log.warn("Error checking if rule matches for query: {}", context.getRecord().getId(), e);
            return false;
        }
    }

    @Override
    public Recommendation generate(QueryContext context) {
        try {
            // Find the problematic field
            String fieldName = null;
            Object queryValue = null;
            boolean isTermsQuery = false;

            // Check term queries first
            List<TermQueryBuilder> termQueries = context.extractTermQueries();
            for (TermQueryBuilder termQuery : termQueries) {
                String field = termQuery.fieldName();
                if (field != null && isTextFieldType(context, field)) {
                    fieldName = field;
                    queryValue = termQuery.value();
                    break;
                }
            }

            // If not found, check terms queries
            if (fieldName == null) {
                List<TermsQueryBuilder> termsQueries = context.extractTermsQueries();
                for (TermsQueryBuilder termsQuery : termsQueries) {
                    String field = termsQuery.fieldName();
                    if (field != null && isTextFieldType(context, field)) {
                        fieldName = field;
                        queryValue = termsQuery.values();
                        isTermsQuery = true;
                        break;
                    }
                }
            }

            if (fieldName == null) {
                log.warn("Could not find problematic field for term-on-text rule");
                return null;
            }

            // Check if field has .keyword subfield
            boolean hasKeywordSubfield = context.hasKeywordSubfield(fieldName);
            String fieldType = context.getFieldType(fieldName);

            // Build the recommendation
            Action action = buildAction(context, fieldName, hasKeywordSubfield, isTermsQuery);
            ImpactVector impact = buildImpact();
            Map<String, Object> metadata = buildMetadata(fieldName, fieldType, hasKeywordSubfield, queryValue, isTermsQuery);

            return Recommendation.builder()
                .id(UUID.randomUUID().toString())
                .ruleId(RULE_ID)
                .title(isTermsQuery ? "Terms query on analyzed text field" : "Term query on analyzed text field")
                .description(buildDescription(fieldName, queryValue, isTermsQuery))
                .type(RecommendationType.CORRECTNESS)
                .action(action)
                .impact(impact)
                .confidence(BASE_CONFIDENCE)
                .metadata(metadata)
                .build();
        } catch (Exception e) {
            log.error("Error generating recommendation for term-on-text rule", e);
            return null;
        }
    }

    private boolean isTextFieldType(QueryContext context, String fieldName) {
        // Skip if field name ends with .keyword (user already knows what they're doing)
        if (fieldName.endsWith(".keyword")) {
            log.info("TermOnTextFieldRule: Skipping field {} - ends with .keyword", fieldName);
            return false;
        }

        String fieldType = context.getFieldType(fieldName);
        log.info("TermOnTextFieldRule: Field {} has type: {}", fieldName, fieldType);
        boolean isText = "text".equals(fieldType) || "match_only_text".equals(fieldType);
        log.info("TermOnTextFieldRule: Field {} isText: {}", fieldName, isText);
        return isText;
    }

    private String buildDescription(String fieldName, Object queryValue, boolean isTermsQuery) {
        String queryType = isTermsQuery ? "Terms" : "Term";
        return String.format(
            Locale.ROOT,
            "%s queries require exact token matches. Text fields are analyzed (lowercased, tokenized), "
                + "so the exact input won't match the stored tokens. This is the #1 cause of 'zero results' confusion. "
                + "For field '%s', your query will likely return no results or incomplete results.",
            queryType,
            fieldName
        );
    }

    private Action buildAction(QueryContext context, String fieldName, boolean hasKeywordSubfield, boolean isTermsQuery) {
        String queryType = isTermsQuery ? "terms" : "term";
        String suggestedQuery = buildSuggestedQuery(context, fieldName, hasKeywordSubfield);

        if (hasKeywordSubfield) {
            return Action.builder()
                .name("use_keyword_subfield")
                .hint(
                    String.format(
                        Locale.ROOT,
                        "Use the .keyword subfield for exact matching: change '%s' to '%s.keyword' in your %s query.",
                        fieldName,
                        fieldName,
                        queryType
                    )
                )
                .suggestedQuery(suggestedQuery)
                .documentationUrl("https://opensearch.org/docs/latest/query-dsl/term/term/")
                .build();
        } else {
            return Action.builder()
                .name("use_match_query")
                .hint(
                    String.format(
                        Locale.ROOT,
                        "Use a match query instead of %s on '%s'. Match queries analyze the input to match stored tokens. "
                            + "Alternatively, add a .keyword subfield to the mapping for true exact matches.",
                        queryType,
                        fieldName
                    )
                )
                .suggestedQuery(suggestedQuery)
                .documentationUrl("https://opensearch.org/docs/latest/query-dsl/term/term/")
                .build();
        }
    }

    /**
     * Build a suggested query by rewriting the original query tree.
     * When hasKeywordSubfield is true, rewrites term/terms to target field.keyword.
     * When false, rewrites term to match (and terms to bool/should of match queries).
     *
     * @param context the query context containing the original SearchSourceBuilder
     * @param targetField the problematic field to rewrite
     * @param useKeyword if true, rewrite to field.keyword; if false, rewrite to match query
     * @return the full search body JSON string, or null if the query cannot be rewritten
     */
    private String buildSuggestedQuery(QueryContext context, String targetField, boolean useKeyword) {
        try {
            SearchSourceBuilder original = context.getSearchSourceBuilder();
            if (original == null || original.query() == null) {
                return null;
            }

            QueryBuilder rewrittenQuery = rewriteQuery(original.query(), targetField, useKeyword);

            SearchSourceBuilder suggested = new SearchSourceBuilder();
            suggested.query(rewrittenQuery);

            return Strings.toString(MediaTypeRegistry.JSON, suggested);
        } catch (Exception e) {
            log.warn("Error building suggested query for field: {}", targetField, e);
            return null;
        }
    }

    /**
     * Recursively walk the query tree and rewrite problematic term/terms nodes.
     *
     * @param query the current query node
     * @param targetField the field name to rewrite
     * @param useKeyword if true, change field to field.keyword; if false, replace with match query
     * @return the rewritten query tree
     */
    private QueryBuilder rewriteQuery(QueryBuilder query, String targetField, boolean useKeyword) {
        if (query instanceof TermQueryBuilder) {
            TermQueryBuilder term = (TermQueryBuilder) query;
            if (targetField.equals(term.fieldName())) {
                if (useKeyword) {
                    return new TermQueryBuilder(targetField + ".keyword", term.value());
                } else {
                    return new MatchQueryBuilder(targetField, term.value());
                }
            }
            return query;
        } else if (query instanceof TermsQueryBuilder) {
            TermsQueryBuilder terms = (TermsQueryBuilder) query;
            if (targetField.equals(terms.fieldName())) {
                if (useKeyword) {
                    return new TermsQueryBuilder(targetField + ".keyword", terms.values().toArray());
                } else {
                    // Convert terms to bool/should with match queries
                    BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                    for (Object val : terms.values()) {
                        boolQuery.should(new MatchQueryBuilder(targetField, val));
                    }
                    boolQuery.minimumShouldMatch(1);
                    return boolQuery;
                }
            }
            return query;
        } else if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder bool = (BoolQueryBuilder) query;
            BoolQueryBuilder newBool = new BoolQueryBuilder();
            for (QueryBuilder q : bool.must()) {
                newBool.must(rewriteQuery(q, targetField, useKeyword));
            }
            for (QueryBuilder q : bool.should()) {
                newBool.should(rewriteQuery(q, targetField, useKeyword));
            }
            for (QueryBuilder q : bool.filter()) {
                newBool.filter(rewriteQuery(q, targetField, useKeyword));
            }
            for (QueryBuilder q : bool.mustNot()) {
                newBool.mustNot(rewriteQuery(q, targetField, useKeyword));
            }
            if (bool.minimumShouldMatch() != null) {
                newBool.minimumShouldMatch(bool.minimumShouldMatch());
            }
            return newBool;
        }
        // Unknown query type - return unchanged
        return query;
    }

    private ImpactVector buildImpact() {
        return ImpactVector.builder()
            .correctness(Direction.INCREASE) // This is the key benefit - query will return correct results
            .latency(Direction.NEUTRAL) // No latency impact (term queries are fast)
            .cpu(Direction.NEUTRAL)
            .memory(Direction.NEUTRAL)
            .confidence(BASE_CONFIDENCE)
            .estimatedImprovement("Returns correct results instead of zero/incomplete results")
            .build();
    }

    private Map<String, Object> buildMetadata(
        String fieldName,
        String fieldType,
        boolean hasKeywordSubfield,
        Object queryValue,
        boolean isTermsQuery
    ) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("field", fieldName);
        metadata.put("field_type", fieldType);
        metadata.put("has_keyword_subfield", hasKeywordSubfield);
        metadata.put("query_type", isTermsQuery ? "terms" : "term");
        if (queryValue != null) {
            metadata.put("query_value_sample", queryValue.toString());
        }
        return metadata;
    }
}
