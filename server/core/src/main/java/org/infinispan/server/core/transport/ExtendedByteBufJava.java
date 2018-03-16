package org.infinispan.server.core.transport;

import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;

/**
 *  Utilities to read from a {@link ByteBuf}
 *
 * @author wburns
 * @since 9.0
 */
public class ExtendedByteBufJava {

   private ExtendedByteBufJava() {
   }

   public static long readUnsignedMaybeLong(ByteBuf buf) {
      if (buf.readableBytes() < 8) {
         buf.resetReaderIndex();
         return Long.MIN_VALUE;
      }
      return buf.readLong();
   }

   public static long readMaybeVLong(ByteBuf buf) {
      if (buf.readableBytes() > 0) {
         byte b = buf.readByte();
         long i = b & 0x7F;
         for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            if (buf.readableBytes() == 0) {
               buf.resetReaderIndex();
               return Long.MIN_VALUE;
            }
            b = buf.readByte();
            i |= (b & 0x7FL) << shift;
         }
         return i;
      } else {
         buf.resetReaderIndex();
         return Long.MIN_VALUE;
      }
   }

   public static int readMaybeVInt(ByteBuf buf) {
      if (buf.readableBytes() > 0) {
         byte b = buf.readByte();
         int i = b & 0x7F;
         for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            if (buf.readableBytes() == 0) {
               buf.resetReaderIndex();
               return Integer.MIN_VALUE;
            }
            b = buf.readByte();
            i |= (b & 0x7FL) << shift;
         }
         return i;
      } else {
         buf.resetReaderIndex();
         return Integer.MIN_VALUE;
      }
   }


   public static byte[] readMaybeRangedBytes(ByteBuf bf) {
      int length = readMaybeVInt(bf);
      if (length == Integer.MIN_VALUE) {
         return null;
      }
      return readMaybeRangedBytes(bf, length);
   }

   public static byte[] readMaybeRangedBytes(ByteBuf bf, int length) {
      if (bf.readableBytes() < length) {
         bf.resetReaderIndex();
         return null;
      } else {
         if (length == 0) {
            return Util.EMPTY_BYTE_ARRAY;
         }
         byte[] bytes = new byte[length];
         bf.readBytes(bytes);
         return bytes;
      }
   }

}
