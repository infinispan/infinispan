package org.infinispan.query;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.hibernate.search.filter.FullTextFilter;
import org.hibernate.search.query.engine.spi.FacetManager;

/**
 * A cache-query is what will be returned when the getQuery() method is run on {@link org.infinispan.query.impl.SearchManagerImpl}. This object can
 * have methods such as list, setFirstResult,setMaxResults, setFetchSize, getResultSize and setSort.
 *
 * @author Manik Surtani
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @see org.infinispan.query.impl.SearchManagerImpl#getQuery(org.apache.lucene.search.Query, Class...)
 */
public interface CacheQuery<E> extends Iterable<E> {

   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    */
   List<E> list();

   /**
    * Returns the results of a search as a {@link ResultIterator}.
    *
    * Warning: the return type is an extension of {@link java.util.Iterator} which introduces a {@link ResultIterator#close()}
    * method. This close method needs to be invoked when the iteration is complete to avoid resource leakage.
    *
    * @param fetchOptions how to fetch results (see @link FetchOptions)
    * @return a QueryResultIterator which can be used to iterate through the results that were found.
    */
   ResultIterator<E> iterator(FetchOptions fetchOptions);

   /**
    * Returns the results of a search as a {@link ResultIterator}. This calls {@link CacheQuery#iterator(FetchOptions fetchOptions)}
    * with default FetchOptions; this implies eager loading of all results.
    *
    * @return a ResultIterator which can be used to iterate through the results that were found.
    */
   @Override
   ResultIterator<E> iterator();

   /**
    * Sets a result with a given index to the first result.
    *
    * @param index of result to be set to the first.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   CacheQuery<E> firstResult(int index);

   /**
    * Sets the maximum number of results to the number passed in as a parameter.
    *
    * @param numResults that are to be set to the maxResults.
    */
   CacheQuery<E> maxResults(int numResults);

   /**
    * @return return the manager for all faceting related operations
    */
   FacetManager getFacetManager();

   /**
    * Gets the total number of results matching the query, ignoring pagination (firstResult, maxResult).
    *
    * @return total number of results.
    */
   int getResultSize();

   /**
    * Return the Lucene {@link org.apache.lucene.search.Explanation}
    * object describing the score computation for the matching object/document
    * in the current query
    *
    * @param documentId Lucene Document id to be explain. This is NOT the object key
    * @return Lucene Explanation
    */
   Explanation explain(int documentId);

   /**
    * Allows lucene to sort the results. Integers are sorted in descending order.
    *
    * @param s - lucene sort object
    */
   CacheQuery<E> sort(Sort s);

   /**
    * Defines the Lucene field names projected and returned in a query result
    * Each field is converted back to it's object representation, an Object[] being returned for each "row"
    * <p>
    * A projectable field must be stored in the Lucene index and use a {@link org.hibernate.search.bridge.TwoWayFieldBridge}
    * Unless notified in their JavaDoc, all built-in bridges are two-way. All @DocumentId fields are projectable by design.
    * <p>
    * If the projected field is not a projectable field, null is returned in the object[]
    *
    * @param fields the projected field names
    * @return {@code this} to allow for method chaining, but the type parameter now becomes {@code Object[]}
    */
   CacheQuery<Object[]> projection(String... fields);

   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   FullTextFilter enableFullTextFilter(String name);

   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   CacheQuery<E> disableFullTextFilter(String name);

   /**
    * Allows lucene to filter the results.
    *
    * @param f - lucene filter
    */
   CacheQuery<E> filter(Filter f);

   /**
    * Set the timeout for this query. If the query hasn't finished processing before the timeout,
    * an exception will be thrown.
    *
    * @param timeout the timeout duration
    * @param timeUnit the time unit of the timeout parameter
    * @return
    */
   CacheQuery<E> timeout(long timeout, TimeUnit timeUnit);
}
