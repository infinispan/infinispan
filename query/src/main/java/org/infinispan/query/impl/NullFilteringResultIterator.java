package org.infinispan.query.impl;

import org.infinispan.query.ResultIterator;

/**
 * @author Marko Luksa
 */
public class NullFilteringResultIterator<E> extends NullFilteringIterator<E> implements ResultIterator<E> {

   private final ResultIterator<E> delegate;

   public NullFilteringResultIterator(ResultIterator<E> delegate) {
      super(delegate);
      this.delegate = delegate;
   }

   @Override
   public void close() {
      delegate.close();
   }
}
