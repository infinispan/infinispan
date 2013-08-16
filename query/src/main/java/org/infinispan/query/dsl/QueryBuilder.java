package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryBuilder extends FilterConditionBeginContext {

   QueryBuilder orderBy(String attributePath, SortOrder sortOrder);

   QueryBuilder setProjection(String... attributePath);

   QueryBuilder startOffset(long startOffset);

   QueryBuilder maxResults(int maxResults);

   Query build();
}
