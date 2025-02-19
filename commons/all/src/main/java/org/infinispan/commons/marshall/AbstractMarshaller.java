package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.LazyByteArrayOutputStream;

/**
 * Abstract Marshaller implementation containing shared implementations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class AbstractMarshaller implements Marshaller {

   protected final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshallableTypeHints.getBufferSizePredictor(o.getClass());
   }

   /**
    * This is a convenience method for converting an object into a {@link org.infinispan.commons.io.ByteBuffer} which takes
    * an estimated size as parameter. A {@link org.infinispan.commons.io.ByteBuffer} allows direct access to the byte
    * array with minimal array copying
    *
    * @param o object to marshall
    * @param estimatedSize an estimate of how large the resulting byte array may be
    */
   protected abstract ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException;

   @Override
   public ByteBuffer objectToBuffer(Object obj) throws IOException, InterruptedException {
      if (obj != null) {
         BufferSizePredictor sizePredictor = marshallableTypeHints
               .getBufferSizePredictor(obj.getClass());
         int estimatedSize = sizePredictor.nextSize(obj);
         ByteBuffer byteBuffer = objectToBuffer(obj, estimatedSize);
         int length = byteBuffer.getLength();
         // If the prediction is way off, then trim it
         if (estimatedSize > (length * 4)) {
            byte[] buffer = trimBuffer(byteBuffer);
            byteBuffer = ByteBufferImpl.create(buffer);
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
         BufferSizePredictor sizePredictor = marshallableTypeHints
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
    * Unmarshall an object from an {@link InputStream}
    *
    * @param inputStream stream to unmarshall from
    * @return the unmarshalled object instance
    * @throws IOException if unmarshalling cannot complete due to some I/O error
    * @throws ClassNotFoundException if the class of the object trying to unmarshall is unknown
    */
   public Object objectFromInputStream(InputStream inputStream) throws IOException, ClassNotFoundException {
      int len = inputStream.available();
      LazyByteArrayOutputStream bytes;
      if(len > 0) {
         bytes = new LazyByteArrayOutputStream(len);
      } else {
         // Some input stream providers do not implement available()
         bytes = new LazyByteArrayOutputStream();
      }
      inputStream.transferTo(bytes);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

   public void objectToOutputStream(Object obj, OutputStream outputStream) throws IOException, InterruptedException {
      byte[] bytes = objectToByteBuffer(obj);
      outputStream.write(bytes);
   }

}
