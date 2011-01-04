package org.infinispan.marshall;

import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract Marshaller implementation containing shared implementations.
 *
 * @author Galder Zamarreño
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
   protected abstract ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException;

   @Override
   public ByteBuffer objectToBuffer(Object obj) throws IOException, InterruptedException {
      return objectToBuffer(obj, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException, InterruptedException {
      return objectToByteBuffer(o, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

   /**
    * This method implements {@link StreamingMarshaller#objectFromInputStream(java.io.InputStream)}, but its
    * implementation has been moved here rather that keeping under a class that implements StreamingMarshaller
    * in order to avoid code duplication.
    */
   public Object objectFromInputStream(InputStream inputStream) throws IOException, ClassNotFoundException {
      int len = inputStream.available();
      ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(len);
      byte[] buf = new byte[Math.min(len, 1024)];
      int bytesRead;
      while ((bytesRead = inputStream.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

}
