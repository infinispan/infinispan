package org.infinispan.commons.api.query;

import org.infinispan.commons.util.CloseableIterator;

public interface ClosableIteratorWithCount<E> extends CloseableIterator<E> {

   /**
    * @return An object containing information about the number of hits from the query, ignoring pagination.
    */
   HitCount count();

}
