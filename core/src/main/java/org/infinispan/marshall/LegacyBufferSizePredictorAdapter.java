package org.infinispan.marshall;

import org.infinispan.commons.marshall.BufferSizePredictor;

/**
 * LegacyBufferSizePredictorAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class LegacyBufferSizePredictorAdapter implements BufferSizePredictor {

   final org.infinispan.marshall.BufferSizePredictor delegate;

   public LegacyBufferSizePredictorAdapter(org.infinispan.marshall.BufferSizePredictor delegate) {
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
