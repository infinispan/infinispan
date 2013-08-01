package org.infinispan.query.dsl;

import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query extends Iterable {

   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    */
   <T> List<T> list();

   /**
    * Returns the results of a search as a {@link org.infinispan.query.ResultIterator}.
    * <p/>
    * Warning: the return type is an extension of {@link java.util.Iterator} which introduces a {@link org.infinispan.query.ResultIterator#close()}
    * method. This close method needs to be invoked when the iteration is complete to avoid resource leakage.
    *
    * @param fetchOptions how to fetch results (see @link FetchOptions)
    * @return a QueryResultIterator which can be used to iterate through the results that were found.
    */
   ResultIterator iterator(FetchOptions fetchOptions);

   /**
    * Returns the results of a search as a {@link ResultIterator}. This calls {@link Query#iterator(FetchOptions fetchOptions)}
    * with default FetchOptions; this implies eager loading of all results.
    *
    * @return a ResultIterator which can be used to iterate through the results that were found.
    */
   @Override
   ResultIterator iterator();

   /**
    * Gets the total number of results matching the query, ignoring pagination (firstResult, maxResult).
    *
    * @return total number of results.
    */
   int getResultSize();
}
