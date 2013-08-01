package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryFactory {

   QueryBuilder from(String typeName);

   FilterConditionEndContext having(String attributePath);

   FilterConditionBeginContext not();
}
