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
    * @param numResults the maximum number of results to return.
    */
   IndexedQuery<E> maxResults(int numResults);

   CloseableIterator<E> iterator();

   // TODO [anistor] works for queries without projections only, should throw exception for projections
   <K> CloseableIterator<Map.Entry<K, E>> entryIterator();

   QueryResult<?> execute();

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
