package org.infinispan.commons.api.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.query.impl.QueryPublisher;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Experimental;
import org.reactivestreams.Publisher;

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
    * Reactive based query for {@link #list()}. Will stream data as requested. The query will poll data
    * from the underlying engine in <b>batchSize</b> blocks of items to satisfy the subscription
    * <p>
    * If {@link #startOffset(long)} or {@link #maxResults(int)} will affect the query by either not
    * returning the first <b>startOffset</b> number of matches and will not return more than <b>maxResults</b>.
    * <p>
    * Note: due to current API limitations it is not safe to use this Query instance while there is an active
    * subscription to the returned Publisher and it is only safe to use one subscription at a time.
    * @param maxBatchSize the maximum amount of entries that will be retrieved at once. Note that a smaller batch size
    *                     may be queried to ensure memory safety.
    * @return a Publisher that when subscribed will query
    */
   @Experimental
   default Publisher<T> publish(int maxBatchSize) {
      return new QueryPublisher<>(this, maxBatchSize);
   }

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
    * Reactive version of {@link #execute()}.
    * <p>
    * Note: due to current API limitations until this stage completes it is not safe to use this Query
    * instance for any other invocations while there is an outstanding Publisher subscription or to have more than
    * one subscription at a time.
    * @return a Stage that when complete contains the query results
    */
   @Experimental
   CompletionStage<QueryResult<T>> executeAsync();

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
    * Reactive version of {@link #executeStatement()}
    * @return a Stage that when complete contains the affected (deleted) entries
    */
   @Experimental
   CompletionStage<Integer> executeStatementAsync();

   /**
    * Indicates if the parsed query has projections (a SELECT clause) and consequently, the returned results will
    * actually be {@code Object[]} containing the projected values rather than the target entity.
    *
    * @return {@code true} if it has projections, {@code false} otherwise.
    */
   boolean hasProjections();

   /**
    * Returns the start offset configured for this query.
    * @return the start offset
    */
   long getStartOffset();

   /**
    * Sets the starting offset into the overall result set. Must be equal or greater than 0.
    * Use it in combination with {@link #maxResults(int)} to implement pagination.
    * @param startOffset the start offset
    * @return <code>this</code>, for method chaining
    */
   Query<T> startOffset(long startOffset);

   /**
    * Returns the maximum number of results configured for this query.
    * @return the maximum number of results
    */
   int getMaxResults();

   /**
    * Sets the maximum number of results to return. Must be equal or greater than 0. When 0 is set, the execution
    * of the query will not return any results, but will still information about the
    * {@link QueryResult#count() total number of hits}. Use it in combination with {@link #startOffset(long)} to
    * implement pagination.
    * @param maxResults the maximum number of results to return
    * @return <code>this</code>, for method chaining
    */
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
    * Returns a {@link ClosableIteratorWithCount} over the results, including both key and value. Close the iterator when
    * you are done with processing the results. The query cannot use projections.
    *
    * @return the results of the query as an iterator.
    */
   default <K> ClosableIteratorWithCount<EntityEntry<K, T>> entryIterator() {
      return entryIterator(false);
   }

   /**
    * Returns a {@link ClosableIteratorWithCount} over the results, including both key and value. Close the iterator when
    * you are done with processing the results. The query cannot use projections.
    *
    * @param withMetadata Whether the cache entry metadata needs to be retrieved
    *
    * @return the results of the query as an iterator.
    */
   <K> ClosableIteratorWithCount<EntityEntry<K, T>> entryIterator(boolean withMetadata);

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

   Query<T> scoreRequired(boolean scoreRequired);

}
