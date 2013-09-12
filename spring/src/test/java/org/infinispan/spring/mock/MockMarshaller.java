package org.infinispan.spring.mock;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

import java.io.IOException;

public final class MockMarshaller implements Marshaller {

   @Override
   public byte[] objectToByteBuffer(final Object obj, final int estimatedSize) throws IOException,
            InterruptedException {
      return null;
   }

   @Override
   public byte[] objectToByteBuffer(final Object obj) throws IOException, InterruptedException {
      return null;
   }

   @Override
   public Object objectFromByteBuffer(final byte[] buf) throws IOException, ClassNotFoundException {
      return null;
   }

   @Override
   public Object objectFromByteBuffer(final byte[] buf, final int offset, final int length)
            throws IOException, ClassNotFoundException {
      return null;
   }

   @Override
   public ByteBuffer objectToBuffer(final Object o) throws IOException, InterruptedException {
      return null;
   }

   @Override
   public boolean isMarshallable(final Object o) {
      return false;
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return null;
   }

}
