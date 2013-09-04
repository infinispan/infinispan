package org.infinispan.query.dsl.embedded;

import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.dsl.Query;

/**
 * A Query kind that offers iteration and lazy/eager loading options.
 * This is currently only available in embedded mode.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface LuceneQuery extends Query, Iterable {

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
}
