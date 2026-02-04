/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service.recommendations;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.insights.core.rules.RecommendationRule;
import org.opensearch.plugin.insights.core.rules.TermOnTextFieldRule;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SourceString;
import org.opensearch.plugin.insights.rules.model.recommendations.Recommendation;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Service that manages query recommendations
 *
 * This service:
 * 1. Maintains a registry of recommendation rules
 * 2. Evaluates rules against query records on-demand
 * 3. Caches generated recommendations by record ID
 * 4. Provides APIs to retrieve recommendations
 */
public class RecommendationService extends AbstractLifecycleComponent {
    private static final Logger log = LogManager.getLogger(RecommendationService.class);
    private static final int MAX_CACHE_SIZE = 1000;

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final List<RecommendationRule> rules;

    // Simple bounded cache keyed by record ID
    private final Map<String, List<Recommendation>> cache;

    private volatile boolean enabled;
    private volatile double minConfidence;
    private volatile int maxCount;
    private volatile Set<String> enabledRules;

    /**
     * Constructor
     * @param clusterService the cluster service
     * @param xContentRegistry the named XContent registry for reconstructing SearchSourceBuilder
     */
    public RecommendationService(ClusterService clusterService, NamedXContentRegistry xContentRegistry) {
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.rules = new ArrayList<>();
        this.cache = new ConcurrentHashMap<>();

        // Initialize settings from cluster
        this.enabled = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_ENABLED);
        this.minConfidence = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_MIN_CONFIDENCE);
        this.maxCount = clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_MAX_COUNT);
        this.enabledRules = new HashSet<>(clusterService.getClusterSettings().get(QueryInsightsSettings.RECOMMENDATIONS_ENABLED_RULES));

        // Register settings update listeners
        clusterService.getClusterSettings().addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_ENABLED, this::setEnabled);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_MIN_CONFIDENCE, this::setMinConfidence);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_MAX_COUNT, this::setMaxCount);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(QueryInsightsSettings.RECOMMENDATIONS_ENABLED_RULES, this::setEnabledRules);

        // Register built-in rules
        registerRule(new TermOnTextFieldRule());

        log.info("RecommendationService initialized with {} rules", rules.size());
    }

    /**
     * Register a recommendation rule
     * @param rule the rule to register
     */
    public void registerRule(RecommendationRule rule) {
        if (rule != null) {
            rules.add(rule);
            // Sort rules by priority (lower values = higher priority)
            rules.sort(Comparator.comparingInt(RecommendationRule::getPriority));
            log.info("Registered recommendation rule: {}", rule.getName());
        }
    }

    /**
     * Generate recommendations for a query record (on-demand).
     * Checks cache first; generates and caches if not found.
     *
     * @param record the query record
     * @return list of recommendations
     */
    public List<Recommendation> generateRecommendations(SearchQueryRecord record) {
        if (!enabled || record == null) {
            return new ArrayList<>();
        }

        // Check cache first
        List<Recommendation> cached = cache.get(record.getId());
        if (cached != null) {
            return cached;
        }

        try {
            // Get SearchSourceBuilder: use in-memory one if available, else reconstruct from SOURCE attribute
            SearchSourceBuilder ssb = record.getSearchSourceBuilder();
            if (ssb == null) {
                ssb = reconstructSearchSourceBuilder(record);
            }
            if (ssb == null) {
                log.debug("No SearchSourceBuilder available for record {}, skipping recommendation generation", record.getId());
                return new ArrayList<>();
            }

            // Build query context
            QueryContext context = new QueryContext(record, ssb, clusterService);

            // Evaluate all rules
            List<Recommendation> recommendations = new ArrayList<>();
            for (RecommendationRule rule : rules) {
                try {
                    if (!isRuleEnabled(rule)) {
                        continue;
                    }
                    if (rule.isEnabled() && rule.matches(context)) {
                        Recommendation recommendation = rule.generate(context);
                        if (recommendation != null) {
                            recommendations.add(recommendation);
                            log.debug("Rule {} generated recommendation for query {}", rule.getId(), record.getId());
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error evaluating rule {} for query {}", rule.getId(), record.getId(), e);
                }
            }

            // Filter and limit
            recommendations = filterRecommendations(recommendations);

            // Cache result (evict oldest entries if needed)
            if (cache.size() >= MAX_CACHE_SIZE) {
                // Remove a random entry to make room (ConcurrentHashMap iteration order is arbitrary)
                String firstKey = cache.keySet().iterator().next();
                cache.remove(firstKey);
            }
            cache.put(record.getId(), recommendations);

            return recommendations;
        } catch (Exception e) {
            log.error("Error generating recommendations for query {}", record.getId(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Analyze a query record and generate recommendations (used by analyze API).
     * This is the entry point for the POST /_insights/recommendations/analyze endpoint.
     *
     * @param record the query record
     * @return list of recommendations
     */
    public List<Recommendation> analyzeQuery(SearchQueryRecord record) {
        if (!enabled || record == null) {
            return new ArrayList<>();
        }

        try {
            if (record.getSearchSourceBuilder() == null) {
                log.warn(
                    "SearchSourceBuilder is null for record {}, skipping recommendation analysis. "
                        + "This is expected behavior - SearchSourceBuilder is only available for in-memory records, not after serialization.",
                    record.getId()
                );
                return new ArrayList<>();
            }

            log.info("Analyzing query record {} for recommendations", record.getId());

            // Build query context
            QueryContext context = new QueryContext(record, record.getSearchSourceBuilder(), clusterService);

            // Evaluate all rules
            List<Recommendation> recommendations = new ArrayList<>();
            for (RecommendationRule rule : rules) {
                try {
                    if (!isRuleEnabled(rule)) {
                        continue;
                    }
                    if (rule.isEnabled() && rule.matches(context)) {
                        Recommendation recommendation = rule.generate(context);
                        if (recommendation != null) {
                            recommendations.add(recommendation);
                            log.debug("Rule {} generated recommendation for query {}", rule.getId(), record.getId());
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error evaluating rule {} for query {}", rule.getId(), record.getId(), e);
                }
            }

            // Filter and limit
            recommendations = filterRecommendations(recommendations);

            log.info("Generated {} recommendations for query {}", recommendations.size(), record.getId());
            return recommendations;
        } catch (Exception e) {
            log.error("Error analyzing query {}", record.getId(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Reconstruct SearchSourceBuilder from the SOURCE attribute string.
     * Used when the in-memory SearchSourceBuilder is null (after serialization/transport).
     *
     * @param record the query record
     * @return reconstructed SearchSourceBuilder, or null if not possible
     */
    private SearchSourceBuilder reconstructSearchSourceBuilder(SearchQueryRecord record) {
        try {
            Object sourceObj = record.getAttributes().get(Attribute.SOURCE);
            if (sourceObj == null) {
                return null;
            }

            String sourceStr;
            if (sourceObj instanceof SourceString) {
                sourceStr = ((SourceString) sourceObj).getValue();
            } else {
                sourceStr = sourceObj.toString();
            }

            if (sourceStr == null || sourceStr.isEmpty()) {
                return null;
            }

            try (
                XContentParser parser = MediaTypeRegistry.JSON.xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, sourceStr)
            ) {
                return SearchSourceBuilder.fromXContent(parser, false);
            }
        } catch (Exception e) {
            log.debug("Failed to reconstruct SearchSourceBuilder from SOURCE attribute for record {}", record.getId(), e);
            return null;
        }
    }

    /**
     * Clear the recommendation cache
     */
    public void clearCache() {
        cache.clear();
        log.info("Cleared recommendation cache");
    }

    /**
     * Get all registered rules
     * @return list of rules
     */
    public List<RecommendationRule> getRules() {
        return new ArrayList<>(rules);
    }

    /**
     * Get enabled status
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set enabled status
     * @param enabled true to enable, false to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("RecommendationService enabled: {}", enabled);
    }

    /**
     * Set minimum confidence threshold
     * @param minConfidence the minimum confidence (0.0 to 1.0)
     */
    public void setMinConfidence(double minConfidence) {
        this.minConfidence = minConfidence;
        log.info("RecommendationService min confidence: {}", minConfidence);
    }

    /**
     * Set maximum count of recommendations to return
     * @param maxCount the maximum count
     */
    public void setMaxCount(int maxCount) {
        this.maxCount = maxCount;
        log.info("RecommendationService max count: {}", maxCount);
    }

    /**
     * Set enabled rules
     * @param enabledRulesList list of enabled rule IDs
     */
    public void setEnabledRules(List<String> enabledRulesList) {
        this.enabledRules = new HashSet<>(enabledRulesList);
        log.info("RecommendationService enabled rules: {}", this.enabledRules);
    }

    /**
     * Check if a rule is enabled via settings
     * @param rule the rule to check
     * @return true if enabled
     */
    private boolean isRuleEnabled(RecommendationRule rule) {
        // If enabled rules list is empty, all rules are enabled
        if (enabledRules.isEmpty()) {
            return true;
        }
        return enabledRules.contains(rule.getId());
    }

    /**
     * Filter recommendations based on settings
     * @param recommendations the list of recommendations
     * @return filtered list
     */
    private List<Recommendation> filterRecommendations(List<Recommendation> recommendations) {
        return recommendations.stream()
            .filter(r -> r.getConfidence() >= minConfidence)
            .sorted((r1, r2) -> Double.compare(r2.getConfidence(), r1.getConfidence())) // Sort by confidence desc
            .limit(maxCount)
            .collect(Collectors.toList());
    }

    /**
     * Get the size of the cache
     * @return the number of cached entries
     */
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    protected void doStart() {
        log.info("Starting RecommendationService with {} rules", rules.size());
    }

    @Override
    protected void doStop() {
        log.info("Stopping RecommendationService");
    }

    @Override
    protected void doClose() {
        clearCache();
        log.info("Closed RecommendationService");
    }
}
