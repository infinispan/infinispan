package org.infinispan.commons.api.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;

/**
 * @since 15.0
 */
public interface Query<T> extends Iterable<T> {

   /**
    * Returns the Ickle query string.
    */
   String getQueryString();

   /**
    * Returns the results of a search as a list.
    *
    * @return a list of objects that were found in the search.
    */
   List<T> list();

   /**
    *  Executes the query (a SELECT statement). Subsequent invocations cause the query to be re-executed.
    *  <p>
    *  Executing a DELETE is also allowed. In this case, no results will be returned, but the number of affected entries
    *  will be returned as the hit count in the {@link QueryResult}.
    *
    * @return {@link QueryResult} with the results.
    */
   QueryResult<T> execute();

   /**
    * Executes a data modifying statement (typically a DELETE) that does not return results; instead it returns the
    * count of affected entries. This method cannot be used to execute a SELECT.
    * <p>
    * <b>NOTE:</b> Paging parameters (firstResult/maxResults) are NOT allowed.
    *
    * @return the number of affected (deleted) entries
    */
   int executeStatement();

   /**
    * Indicates if the parsed query has projections (a SELECT clause) and consequently, the returned results will
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
    * @return the current hitCountAccuracy if present
    * @see #hitCountAccuracy(int)
    */
   Integer hitCountAccuracy();

   /**
    * Limits the required accuracy of the hit count for the indexed queries to an upper-bound.
    * Setting the hit-count-accuracy could improve the performance of queries targeting large data sets.
    *
    * @param hitCountAccuracy The value to apply
    * @return <code>this</code>, for method chaining
    */
   Query<T> hitCountAccuracy(int hitCountAccuracy);

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
    * Sets multiple named parameters at once. Parameter names cannot be empty or {@code null}. Parameter values must
    * not be {@code null}.
    *
    * @param paramValues a Map of parameters
    * @return itself
    */
   Query<T> setParameters(Map<String, Object> paramValues);

   /**
    * Returns a {@link CloseableIterator} over the results. Close the iterator when you are done with processing
    * the results.
    *
    * @return the results of the query as an iterator.
    */
   @Override
   CloseableIterator<T> iterator();

   /**
    * Returns a {@link CloseableIterator} over the results, including both key and value. Close the iterator when
    * you are done with processing the results. The query cannot use projections.
    *
    * @return the results of the query as an iterator.
    */
   <K> CloseableIterator<Map.Entry<K, T>> entryIterator();

   /**
    * Set the timeout for this query. If the query hasn't finished processing before the timeout,
    * a timeout will be thrown. For queries that use the index, the timeout
    * is handled on a best effort basis, and the supplied time is rounded to the nearest millisecond.
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
