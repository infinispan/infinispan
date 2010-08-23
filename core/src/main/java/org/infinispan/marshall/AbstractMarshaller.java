package org.infinispan.marshall;

import org.infinispan.io.ByteBuffer;

import java.io.IOException;

/**
 * Abstract Marshaller implementation containing shared implementations.
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
public abstract class AbstractMarshaller implements Marshaller {

   protected static final int DEFAULT_BUF_SIZE = 512;

   /**
    * This is a convenience method for converting an object into a {@link org.infinispan.io.ByteBuffer} which takes
    * an estimated size as parameter. A {@link org.infinispan.io.ByteBuffer} allows direct access to the byte
    * array with minimal array copying
    *
    * @param o object to marshall
    * @param estimatedSize an estimate of how large the resulting byte array may be
    * @return a ByteBuffer
    * @throws Exception
    */
   protected abstract ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException;

   @Override
   public ByteBuffer objectToBuffer(Object obj) throws IOException {
      return objectToBuffer(obj, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException {
      return objectToByteBuffer(o, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

}
