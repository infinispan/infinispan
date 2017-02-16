package org.infinispan.query.dsl;

/**
 * A builder for {@link Query} objects. An instance of this class can be obtained from {@link QueryFactory}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface QueryBuilder extends FilterConditionBeginContext, PaginationContext<QueryBuilder> {

   QueryBuilder orderBy(Expression expression);

   QueryBuilder orderBy(Expression expression, SortOrder sortOrder);

   QueryBuilder orderBy(String attributePath);

   QueryBuilder orderBy(String attributePath, SortOrder sortOrder);

   QueryBuilder select(Expression... projection);

   QueryBuilder select(String... attributePath);

   QueryBuilder groupBy(String... attributePath);

   /**
    * Builds the query object. Once built, the query is immutable (except for the named parameters).
    *
    * @return the Query
    */
   Query build();
}
