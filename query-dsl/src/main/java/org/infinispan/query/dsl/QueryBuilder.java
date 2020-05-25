package org.infinispan.query.dsl;

/**
 * A builder for {@link Query} objects. An instance of this class can be obtained from {@link QueryFactory}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated since 10.1. The Ickle query language is now preferred over the {@code QueryBuilder}. See {@link
 * QueryFactory#create}. The {@code QueryBuilder} and associated context interfaces will be removed in version 11.0.
 */
@Deprecated
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
   <T> Query<T> build();
}
