package org.infinispan.marshall;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.io.ByteBufferImpl;
import org.infinispan.io.ExposedByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract Marshaller implementation containing shared implementations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 * @deprecated use {@link org.infinispan.commons.marshall.AbstractMarshaller} instead
 */
@Deprecated
public abstract class AbstractMarshaller implements Marshaller {

   protected final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return new CommonsBufferSizePredictorAdapter(marshallableTypeHints.getBufferSizePredictor(o.getClass()));
   }

   /**
    * This is a convenience method for converting an object into a {@link org.infinispan.io.ByteBufferImpl} which takes
    * an estimated size as parameter. A {@link org.infinispan.io.ByteBufferImpl} allows direct access to the byte
    * array with minimal array copying
    *
    * @param o object to marshall
    * @param estimatedSize an estimate of how large the resulting byte array may be
    * @return a ByteBufferImpl
    * @throws Exception
    */
   protected abstract ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException;

   @Override
   public ByteBuffer objectToBuffer(Object obj) throws IOException, InterruptedException {
      if (obj != null) {
         org.infinispan.commons.marshall.BufferSizePredictor sizePredictor = marshallableTypeHints
               .getBufferSizePredictor(obj.getClass());
         int estimatedSize = sizePredictor.nextSize(obj);
         ByteBuffer byteBuffer = objectToBuffer(obj, estimatedSize);
         int length = byteBuffer.getLength();
         // If the prediction is way off, then trim it
         if (estimatedSize > (length * 4)) {
            byte[] buffer = trimBuffer(byteBuffer);
            byteBuffer = new ByteBufferImpl(buffer, 0, buffer.length);
         }
         sizePredictor.recordSize(length);
         return byteBuffer;
      } else {
         return objectToBuffer(null, 1);
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException, InterruptedException {
      if (o != null) {
         org.infinispan.commons.marshall.BufferSizePredictor sizePredictor = marshallableTypeHints
               .getBufferSizePredictor(o.getClass());
         byte[] bytes = objectToByteBuffer(o, sizePredictor.nextSize(o));
         sizePredictor.recordSize(bytes.length);
         return bytes;
      } else {
         return objectToByteBuffer(null, 1);
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      return trimBuffer(b);
   }

   private byte[] trimBuffer(ByteBuffer b) {
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
      ExposedByteArrayOutputStream bytes;
      byte[] buf;
      if(len > 0) {
         bytes = new ExposedByteArrayOutputStream(len);
         buf = new byte[Math.min(len, 1024)];
      } else {
         // Some input stream providers do not implement available()
         bytes = new ExposedByteArrayOutputStream();
         buf = new byte[1024];
      }
      int bytesRead;
      while ((bytesRead = inputStream.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

}
