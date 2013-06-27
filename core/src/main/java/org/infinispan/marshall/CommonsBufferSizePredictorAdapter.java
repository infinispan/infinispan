package org.infinispan.marshall;

/**
 * CommonsBufferSizePredictorAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class CommonsBufferSizePredictorAdapter implements BufferSizePredictor {

   final org.infinispan.commons.marshall.BufferSizePredictor delegate;

   public CommonsBufferSizePredictorAdapter(org.infinispan.commons.marshall.BufferSizePredictor delegate) {
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
