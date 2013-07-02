package org.infinispan.query.impl;

import org.infinispan.query.ResultIterator;

/**
 * @author Marko Luksa
 */
public class NullFilteringResultIterator extends NullFilteringIterator<Object> implements ResultIterator {

   private final ResultIterator delegate;

   public NullFilteringResultIterator(ResultIterator delegate) {
      super(delegate);
      this.delegate = delegate;
   }

   @Override
   public void close() {
      delegate.close();
   }
}
