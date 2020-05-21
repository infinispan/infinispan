package org.infinispan.query.dsl;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.CloseableIterator;

//todo [anistor] We need to deprecate the 'always caching' behaviour and provide a clearCachedResults method

/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results. If you intend to re-execute the query to obtain fresh data you need to build another
 * instance using a {@link QueryBuilder}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query<T> extends Iterable<T>, PaginationContext<Query<T>>, ParameterContext<Query<T>> {

   /**
    * Returns the Ickle query string.
    */
   String getQueryString();

   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    * @deprecated since 11.0, use {@link QueryResult#list()} instead.
    */
   @Deprecated
   List<T> list();

   /**
    *  Executes the query. Subsequent invocations cause the query to be re-executed.
    *
    * @return {@link QueryResult} with the results.
    * @since 11.0
    */
   QueryResult<T> execute();

   /**
    * Gets the total number of results matching the query, ignoring pagination (firstResult, maxResult).
    *
    * @return total number of results.
    * @deprecated since 10.1. This will be removed with no direct replacement.
    */
   @Deprecated
   int getResultSize(); //todo [anistor] this should probably be a long?

   /**
    * @return the values for query projections or {@code null} if the query does not have projections.
    */
   String[] getProjection();

   long getStartOffset();

   Query<T> startOffset(long startOffset);

   int getMaxResults();

   Query<T> maxResults(int maxResults);

   /**
    * Returns the named parameters Map.
    *
    * @return the named parameters (unmodifiable) or {@code null} if the query does not have parameters
    */
   Map<String, Object> getParameters();

   /**
    * Sets the value of a named parameter.
    *
    * @param paramName  the parameters name (non-empty and not null)
    * @param paramValue a non-null value
    * @return itself
    */
   Query<T> setParameter(String paramName, Object paramValue);

   /**
    * Sets multiple named parameters at once. Parameters names cannot be empty or {@code null}. Parameter values must
    * not be {@code null}.
    *
    * @param paramValues a Map of parameters
    * @return itself
    */
   Query<T> setParameters(Map<String, Object> paramValues);

   /**
    * @return the results of the query as an iterator.
    */
   CloseableIterator<T> iterator();
}
