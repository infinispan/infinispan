package org.infinispan.query.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;

/**
 * A distributed Lucene query.
 *
 * @since 11.0
 */
public interface IndexedQuery<E> {

   /***
    * @return the results of a search as a list.
    */
   List<E> list();

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
