package org.infinispan.query.impl;

import org.infinispan.query.ResultIterator;

/**
 * @author Marko Luksa
 */
final class NullFilteringResultIterator<E> extends NullFilteringIterator<E> implements ResultIterator<E> {

   NullFilteringResultIterator(ResultIterator<E> delegate) {
      super(delegate);
   }

   @Override
   public void close() {
      ((ResultIterator<E>) delegate).close();
   }
}
