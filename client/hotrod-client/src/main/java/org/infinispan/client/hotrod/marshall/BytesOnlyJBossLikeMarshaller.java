package org.infinispan.client.hotrod.marshall;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Marshaller that only supports byte[] instances written in JBoss marshaller encoding. That is that this class
 * will serialize a given byte[] in the same way that org.jboss.marshalling.river(RiverMarshaller|RiverUnmarshaller)
 * would. However this class does not load any jboss marshalling classes, and thus can be used in environments where
 * it is not desired or cannot be used.
 * @author wburns
 * @since 10.0
 */
public class BytesOnlyJBossLikeMarshaller implements Marshaller {
   public static BytesOnlyJBossLikeMarshaller INSTANCE = new BytesOnlyJBossLikeMarshaller();

   private BytesOnlyJBossLikeMarshaller() { }

   // These magic numbers are taken from org.jboss.marshalling.river.Protocol
   private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

   private static final int ID_NULL                     = 0x01;
   private static final int ID_PRIM_BYTE                = 0x21; // ..etc..

   private static final int ID_ARRAY_EMPTY_UNSHARED     = 0x45; // zero elements (CACHED)
   private static final int ID_ARRAY_SMALL_UNSHARED     = 0x46; // <=0x100 elements (CACHED)
   private static final int ID_ARRAY_MEDIUM_UNSHARED    = 0x47; // <=0x10000 elements (CACHED)
   private static final int ID_ARRAY_LARGE_UNSHARED     = 0x48; // <0x80000000 elements (CACHED)

   private void checkByteArray(Object o) {
      if (!(o instanceof byte[])) {
         throw new IllegalArgumentException("Only byte[] instances are supported currently!");
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) {
      checkByteArray(obj);
      return serialize((byte[]) obj);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) {
      checkByteArray(obj);
      return serialize((byte[]) obj);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) {
      return deserialize(buf, 0, buf.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      return deserialize(buf, offset, length);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) {
      checkByteArray(o);
      return new ByteBufferImpl(serialize((byte[]) o));
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof byte[];
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return BYTE_ARRAY_ESTIMATOR;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_JBOSS_MARSHALLING;
   }

   private byte[] serialize(byte[] bytes) {
      if (bytes == null) {
         return new byte[] { ID_NULL };
      }
      int len = bytes.length;
      byte[] result;
      if (len == 0) {
         result = new byte[] {ID_ARRAY_EMPTY_UNSHARED, ID_PRIM_BYTE};
      } else if (len <= 256) {
         result = new byte[3 + len];
         result[0] = ID_ARRAY_SMALL_UNSHARED;
         result[1] = (byte) len;
         result[2] = ID_PRIM_BYTE;
         System.arraycopy(bytes, 0, result, 3, len);
      } else if (len <= 65536) {
         result = new byte[4 + len];
         result[0] = ID_ARRAY_MEDIUM_UNSHARED;
         result[1] = (byte) (len >> 8);
         result[2] = (byte) len;
         result[3] = ID_PRIM_BYTE;
         System.arraycopy(bytes, 0, result, 4, len);
      } else {
         result = new byte[6 + len];
         result[0] = ID_ARRAY_LARGE_UNSHARED;
         result[1] = (byte) (len >> 24);
         result[2] = (byte) (len >> 16);
         result[3] = (byte) (len >> 8);
         result[4] = (byte) len;
         result[5] = ID_PRIM_BYTE;
         System.arraycopy(bytes, 0, result, 6, len);
      }
      return result;
   }

   private byte[] deserialize(byte[] bytes, int offset, int len) {
      byte[] result;
      int id = bytes[offset];
      switch (id) {
         case ID_NULL:
            result = null;
            break;
         case ID_ARRAY_EMPTY_UNSHARED:
            result = EMPTY_BYTE_ARRAY;
            break;
         case ID_ARRAY_SMALL_UNSHARED:
            int size = bytes[offset + 1] & 0xFF;
            // We can ignore offset + 2 as we know it is ID_PRIM_BYTE
            assert size == len - 3;
            result = new byte[size];
            System.arraycopy(bytes, offset + 3, result, 0, result.length);
            break;
         case ID_ARRAY_MEDIUM_UNSHARED:
            size = (bytes[offset + 1] & 0xFF) << 8 | bytes[offset + 2] & 0xFF;
            // We can ignore offset + 3 as we know it is ID_PRIM_BYTE
            assert size == len - 4;
            result = new byte[size];
            System.arraycopy(bytes, offset + 4, result, 0, result.length);
            break;
         case ID_ARRAY_LARGE_UNSHARED:
            size = (bytes[offset + 1] & 0xFF) << 24 | (bytes[offset + 2] & 0xFF) << 16 |
                  (bytes[offset + 3] & 0xFF) << 8 | bytes[offset + 4] & 0xFF;
            // We can ignore offset + 5 as we know it is ID_PRIM_BYTE
            assert size == len - 6;
            result = new byte[size];
            System.arraycopy(bytes, offset + 6, result, 0, result.length);
            break;
         default:
            throw new IllegalArgumentException("Unsupported id: " + id);

      }
      return result;
   }

   private static final ByteArrayEstimator BYTE_ARRAY_ESTIMATOR = new ByteArrayEstimator();

   private static final class ByteArrayEstimator implements BufferSizePredictor {
      @Override
      public int nextSize(Object obj) {
         byte[] bytes = (byte[]) obj;
         int len = bytes.length;
         if (len == 0) {
            return 2;
         } else if (len < 256) {
            return 3 + len;
         } else if (len < 65536) {
            return 4 + len;
         } else {
            return 6 + len;
         }
      }

      @Override
      public void recordSize(int previousSize) {

      }
   }
}
