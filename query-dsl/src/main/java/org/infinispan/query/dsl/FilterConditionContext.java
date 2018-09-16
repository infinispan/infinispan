package org.infinispan.query.dsl;

/**
 * The context of a complete filter. Provides operations to allow connecting multiple filters together with boolean
 * operators.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface FilterConditionContext {

   /**
    * Creates a new context and connects it with the current one using boolean AND. The new context is added after the
    * current one. The two conditions are not grouped so operator precedence in the resulting condition might change.
    * <p>
    * The effect is: a AND b
    *
    * @return the new context
    */
   FilterConditionBeginContext and();

   /**
    * Connects a given context with the current one using boolean AND. The new context is added after the current one
    * and is grouped. Operator precedence will be unaffected due to grouping.
    * <p>
    * The effect is: a AND (b)
    *
    * @param rightCondition the second condition
    * @return the new context
    */
   FilterConditionContextQueryBuilder and(FilterConditionContext rightCondition);

   /**
    * Creates a new context and connects it with the current one using boolean OR. The new context is added after the
    * current one.
    * <p>
    * The effect is: a OR b
    *
    * @return the new context
    */
   FilterConditionBeginContext or();

   /**
    * Connects a given context with the current one using boolean OR. The new context is added after the current one and
    * is grouped.
    * <p>
    * The effect is: a OR (b)
    *
    * @param rightCondition the second condition
    * @return the new context
    */
   FilterConditionContextQueryBuilder or(FilterConditionContext rightCondition);

   /**
    * Get the {@link QueryBuilder} that created this context. As of Infinispan 9.0 this is no longer needed.
    *
    * @return the parent builder
    * @deprecated To be removed in Infinispan 10.0 without replacement.
    */
   @Deprecated
   QueryBuilder toBuilder();
}
