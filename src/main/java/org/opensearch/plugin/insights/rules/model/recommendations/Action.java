/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.recommendations;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Represents a recommended action to take
 */
public class Action implements ToXContentObject {
    private final String type;
    private final String hint;
    private final String documentation;
    private final String codeExample;

    private Action(Builder builder) {
        this.type = builder.type;
        this.hint = builder.hint;
        this.documentation = builder.documentation;
        this.codeExample = builder.codeExample;
    }

    /**
     * @return the action type
     */
    public String getType() {
        return type;
    }

    /**
     * @return the hint describing the action
     */
    public String getHint() {
        return hint;
    }

    /**
     * @return the documentation URL
     */
    public String getDocumentation() {
        return documentation;
    }

    /**
     * @return the code example
     */
    public String getCodeExample() {
        return codeExample;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (type != null) {
            builder.field("type", type);
        }
        if (hint != null) {
            builder.field("hint", hint);
        }
        if (documentation != null) {
            builder.field("documentation", documentation);
        }
        if (codeExample != null) {
            builder.field("code_example", codeExample);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action action = (Action) o;
        return Objects.equals(type, action.type)
            && Objects.equals(hint, action.hint)
            && Objects.equals(documentation, action.documentation)
            && Objects.equals(codeExample, action.codeExample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, hint, documentation, codeExample);
    }

    /**
     * Builder for Action
     */
    public static class Builder {
        private String type;
        private String hint;
        private String documentation;
        private String codeExample;

        /**
         * Set the action type
         * @param type the type
         * @return this builder
         */
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        /**
         * Set the hint
         * @param hint the hint
         * @return this builder
         */
        public Builder hint(String hint) {
            this.hint = hint;
            return this;
        }

        /**
         * Set the documentation URL
         * @param documentation the URL
         * @return this builder
         */
        public Builder documentation(String documentation) {
            this.documentation = documentation;
            return this;
        }

        /**
         * Set the code example
         * @param codeExample the code example
         * @return this builder
         */
        public Builder codeExample(String codeExample) {
            this.codeExample = codeExample;
            return this;
        }

        /**
         * Build the Action
         * @return the action
         */
        public Action build() {
            return new Action(this);
        }
    }

    /**
     * Create a new builder
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }
}
