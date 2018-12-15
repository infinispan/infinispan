package org.infinispan.client.hotrod.marshall;

import java.util.Arrays;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Marshaller that only supports byte[] instances writing them as is
 *
 * @author Tristan Tarrant
 * @author wburns
 * @since 10.0
 */
public class BytesOnlyMarshaller implements Marshaller {
   private BytesOnlyMarshaller() { }

   public static final BytesOnlyMarshaller INSTANCE = new BytesOnlyMarshaller();

   private static final BufferSizePredictor predictor = new IdentityBufferSizePredictor();

   private void checkByteArray(Object o) {
      if (!(o instanceof byte[])) {
         throw new IllegalArgumentException("Only byte[] instances are supported currently!");
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) {
      checkByteArray(obj);
      return (byte[]) obj;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) {
      checkByteArray(obj);
      return (byte[]) obj;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) {
      return buf;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      if (offset == 0 && length == buf.length) {
         return buf;
      }
      return Arrays.copyOfRange(buf, offset, offset + length);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) {
      checkByteArray(o);
      byte[] b = (byte[]) o;
      return new ByteBufferImpl(b, 0, b.length);
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof byte[];
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return predictor;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_OCTET_STREAM;
   }

   private static final class IdentityBufferSizePredictor implements BufferSizePredictor {

      @Override
      public int nextSize(Object obj) {
         return ((byte[]) obj).length;
      }

      @Override
      public void recordSize(int previousSize) {
         // NOOP
      }

   }
}
