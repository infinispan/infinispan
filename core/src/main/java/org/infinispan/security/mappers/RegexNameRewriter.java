package org.infinispan.security.mappers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wildfly.common.Assert;

/**
 * A simple regular expression-based name rewriter.
 */
public final class RegexNameRewriter implements NameRewriter {
    private final Pattern pattern;
    private final String replacement;
    private final boolean replaceAll;

    /**
     * Construct a new instance.
     *
     * @param pattern the substitution pattern (must not be {@code null})
     * @param replacement the replacement string
     * @param replaceAll {@code true} to replace all occurrences of the pattern; {@code false} to replace only the first occurrence
     */
    public RegexNameRewriter(final Pattern pattern, final String replacement, final boolean replaceAll) {
        this.pattern = Assert.checkNotNullParam("pattern", pattern);
        this.replacement = replacement;
        this.replaceAll = replaceAll;
    }

    /**
     * Rewrite a name.  Must not return {@code null}.
     *
     * @param original the original name
     *
     * @return the rewritten name
     */
    public String rewriteName(final String original) {
        if (original == null) return null;
        final Matcher matcher = pattern.matcher(original);
        return replaceAll ? matcher.replaceAll(replacement) : matcher.replaceFirst(replacement);
    }

    /**
     * Get the pattern.
     *
     * @return the pattern
     */
    public Pattern getPattern() {
        return pattern;
    }

    /**
     * Get the replacement string.
     *
     * @return the replacement string
     */
    public String getReplacement() {
        return replacement;
    }

    /**
     * Whether this rewriter replaces all occurrences of the patter or just the first
     */
    public boolean isReplaceAll() {
        return replaceAll;
    }
}
