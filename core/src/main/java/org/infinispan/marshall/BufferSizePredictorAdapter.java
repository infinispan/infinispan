package org.infinispan.marshall;

/**
 * BufferSizePredictorAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class BufferSizePredictorAdapter implements BufferSizePredictor {

   final org.infinispan.commons.marshall.BufferSizePredictor delegate;

   public BufferSizePredictorAdapter(org.infinispan.commons.marshall.BufferSizePredictor delegate) {
      this.delegate = delegate;
   }

   @Override
   public int nextSize(Object obj) {
      return delegate.nextSize(obj);
   }

   @Override
   public void recordSize(int previousSize) {
      delegate.recordSize(previousSize);
   }

}
