package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface RangeConditionContext extends FilterConditionContext {

   RangeConditionContext includeLower(boolean includeLower);

   RangeConditionContext includeUpper(boolean includeUpper);
}
