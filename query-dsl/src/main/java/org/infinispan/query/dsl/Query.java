package org.infinispan.query.dsl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.SearchTimeoutException;

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
    * Gets the total number of results matching the query, ignoring pagination (startOffset, maxResults).
    *
    * @return total number of results.
    * @deprecated since 10.1. This will be removed in 12. It's closest replacement is {@link QueryResult#hitCount()}
    * which returns an optional long.
    */
   @Deprecated
   int getResultSize();

   /**
    * @return the values for query projections or {@code null} if the query does not have projections.
    * @deprecated since 11.0. This method will be removed in next major version. To find out if a query uses projections use {@link #hasProjections()}
    */
   @Deprecated
   String[] getProjection();

   /**
    * Indicates if the parsed query has projections (a SELECT clause) and consequently the returned results will
    * actually be {@code Object[]} containing the projected values rather than the target entity.
    *
    * @return {@code true} if it has projections, {@code false} otherwise.
    */
   boolean hasProjections();

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
    * Returns a {@link CloseableIterator} over the results. Please close the iterator when you are done with processing
    * the results.
    *
    * @return the results of the query as an iterator.
    */
   CloseableIterator<T> iterator();

   /**
    *  Set the timeout for this query. If the query hasn't finished processing before the timeout,
    *  a {@link SearchTimeoutException} will be thrown. For queries that use the index, the timeout
    *  is handled on a best effort basis, and the supplied time is rounded to the nearest millisecond.
    */
   Query<T> timeout(long timeout, TimeUnit timeUnit);

   /**
    * Set the query execution scope
    *
    * @param local if true, query will be restricted to the data present in the local node, ignoring the other
    *                  members of the clusters
    */
   Query<T> local(boolean local);
}
