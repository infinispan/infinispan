package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface FilterConditionBeginContext {

   FilterConditionEndContext having(String attributePath);

   FilterConditionBeginContext not();

   FilterConditionContext not(FilterConditionContext fcc);
}
