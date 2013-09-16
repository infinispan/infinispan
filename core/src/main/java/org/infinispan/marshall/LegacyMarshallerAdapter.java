package org.infinispan.marshall;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;

import java.io.IOException;

/**
 * LegacyMarshallerAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class LegacyMarshallerAdapter implements Marshaller {

   final org.infinispan.marshall.Marshaller delegate;

   public LegacyMarshallerAdapter(org.infinispan.marshall.Marshaller delegate) {
      this.delegate = delegate;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return delegate.objectToByteBuffer(obj, estimatedSize);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return delegate.objectToBuffer(o);
   }

   @Override
   public org.infinispan.commons.marshall.BufferSizePredictor getBufferSizePredictor(Object o) {
      return new LegacyBufferSizePredictorAdapter(delegate.getBufferSizePredictor(o));
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return delegate.objectToByteBuffer(obj);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return delegate.objectFromByteBuffer(buf);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return delegate.objectFromByteBuffer(buf, offset, length);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return delegate.isMarshallable(o);
   }
}
