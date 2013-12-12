package org.infinispan.query.dsl;

/**
 * A builder for {@link Query} objects. An instance of this class can be obtained from {@link QueryFactory}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryBuilder<T extends Query> extends FilterConditionBeginContext {

   QueryBuilder orderBy(String attributePath, SortOrder sortOrder);

   QueryBuilder setProjection(String... attributePath);

   QueryBuilder startOffset(long startOffset);

   QueryBuilder maxResults(int maxResults);

   /**
    * Builds the query object. Once built, the query is immutable and can be executed only once.
    *
    * @return the Query
    */
   T build();
}
