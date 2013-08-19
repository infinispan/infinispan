package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryFactory<T extends Query> {

   QueryBuilder<T> from(Class entityType);

   FilterConditionEndContext having(String attributePath);

   FilterConditionBeginContext not();

   FilterConditionContext not(FilterConditionContext fcc);
}
