package org.infinispan.query.dsl;

/**
 * A context for ranges. Allow specifying if the bounds are included or not. They are included by default. This context
 * is considered completed.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface RangeConditionContext extends FilterConditionContext {

   RangeConditionContext includeLower(boolean includeLower);

   RangeConditionContext includeUpper(boolean includeUpper);
}
