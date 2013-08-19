package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface FilterConditionContext {

   FilterConditionBeginContext and();

   FilterConditionContext and(FilterConditionContext rightCondition);

   FilterConditionBeginContext or();

   FilterConditionContext or(FilterConditionContext rightCondition);

   <T extends Query> QueryBuilder<T> toBuilder();
}
