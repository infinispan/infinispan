package org.infinispan.api.query.functions;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Evaluation context handed to {@link QueryFunction#evaluate}: the materialized
 * target entity, a reader for its current property values and the named
 * parameters of the query.
 *
 * @since 16.4
 */
public interface QueryFunctionContext {

    /**
     * Returns the materialized target entity the statement is being applied to.
     *
     * @return the target entity, possibly {@code null} when the expression does not
     *         depend on the entity
     */
    Object getEntity();

    /**
     * Returns the named parameters of the query.
     *
     * @return the named parameters, possibly empty
     */
    Map<String, Object> getNamedParameters();

    /**
     * Reads the current value of the given property path from the target entity.
     *
     * @param path the property path, e.g. {@code ["address", "city"]}
     * @return the current value, possibly {@code null}
     */
    Object getPropertyValue(String[] path);

    /**
     * Returns a compiled {@link Pattern} for the given regex string. Implementations
     * may cache compiled patterns, so prefer this method over
     * {@link Pattern#compile(String)} when compiling the same pattern repeatedly.
     *
     * @param regex the regular-expression string
     * @return the compiled pattern
     */
    Pattern compilePattern(String regex);
}
