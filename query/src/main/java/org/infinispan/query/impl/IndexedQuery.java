package org.infinispan.query.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Sort;
import org.infinispan.query.CacheQuery;

/**
 * A distributed Lucene query.
 *
 * @since 11.0
 */
public interface IndexedQuery<E> extends CacheQuery<E> {

   IndexedQuery<E> sort(Sort s);

   /**
    * Defines the Lucene field names projected and returned in a query result
    * Each field is converted back to it's object representation, an Object[] being returned for each "row"
    * <p/>
    * A projectable field must be stored in the Lucene index and use a {@link org.hibernate.search.bridge.TwoWayFieldBridge}
    * Unless notified in their JavaDoc, all built-in bridges are two-way. All @DocumentId fields are projectable by design.
    * <p/>
    * If the projected field is not a projectable field, null is returned in the object[]
    *
    * @param fields the projected field names
    * @return {@code this} to allow for method chaining, but the type parameter now becomes {@code Object[]}
    */
   IndexedQuery<Object[]> projection(String... fields);

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

   /**
    * Set the timeout for this query. If the query hasn't finished processing before the timeout,
    * an exception will be thrown.
    *
    * @param timeout the timeout duration
    * @param timeUnit the time unit of the timeout parameter
    */
   IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit);
}
