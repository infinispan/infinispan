package org.infinispan.query.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.QueryResult;

/**
 * A query that uses indexing.
 *
 * @since 11.0
 */
public interface IndexedQuery<E> {

   /**
    * @return the results of a search as a list.
    */
   default List<E> list() {
      return (List<E>) execute().list();
   }

   /**
    * Sets the index of the first result, skipping the previous ones. Used for pagination.
    *
    * @param index of the first result
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   IndexedQuery<E> firstResult(int index);

   /**
    * Sets the maximum number of results to return from the query. Used for pagination.
    *
    * @param maxResults the maximum number of results to return.
    */
   IndexedQuery<E> maxResults(int maxResults);

   CloseableIterator<E> iterator();

   /**
    * Returns the matching entries (both key and value).
    * <p>
    * <b>NOTE:</b> The query must not contain any projections or an exception will be thrown.
    */
   <K> CloseableIterator<Map.Entry<K, E>> entryIterator();

   /**
    * Executes an Ickle statement returning results (query aka. SELECT). If the statement happens to be a DELETE it
    * redirects it to {@link #executeStatement()}.
    * <p>
    * <b>NOTE:</b> Paging params (firstResult/maxResults) are honoured for SELECT and dissalowed for DELETE.
    */
   QueryResult<?> execute();

   /**
    * Executes an Ickle statement not returning any results (ie. DELETE).
    * <p>
    * <b>NOTE:</b> Paging params (firstResult/maxResults) are NOT allowed.
    *
    * @return the number of affected entries
    */
   int executeStatement();

   int getResultSize();

   /**
    * Set the timeout for this query. If the query hasn't finished processing before the timeout,
    * an exception will be thrown.
    *
    * @param timeout the timeout duration
    * @param timeUnit the time unit of the timeout parameter
    */
   IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit);
}
