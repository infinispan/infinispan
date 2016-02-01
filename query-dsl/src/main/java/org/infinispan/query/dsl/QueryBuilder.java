package org.infinispan.query.dsl;

/**
 * A builder for {@link Query} objects. An instance of this class can be obtained from {@link QueryFactory}.
 * <p>
 * Type parameter Q will be removed in Infinispan 9.0
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryBuilder extends FilterConditionBeginContext {

   QueryBuilder orderBy(Expression expression);

   QueryBuilder orderBy(Expression expression, SortOrder sortOrder);

   QueryBuilder orderBy(String attributePath);

   QueryBuilder orderBy(String attributePath, SortOrder sortOrder);

   QueryBuilder select(Expression... projection);

   QueryBuilder select(String... attributePath);

   QueryBuilder groupBy(String... attributePath);

   QueryBuilder startOffset(long startOffset);

   QueryBuilder maxResults(int maxResults);

   /**
    * Builds the query object. Once built, the query is immutable and can be executed only once.
    *
    * @return the Query
    */
   Query build();
}
