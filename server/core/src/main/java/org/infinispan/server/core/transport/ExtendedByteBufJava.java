package org.infinispan.server.core.transport;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.netty.VarintEncodeDecode;
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
         return VarintEncodeDecode.readVLong(buf);
      } else {
         buf.resetReaderIndex();
         return Long.MIN_VALUE;
      }
   }

   public static int readMaybeVInt(ByteBuf buf) {
      return VarintEncodeDecode.readVInt(buf);
   }

   public static String readString(ByteBuf bf) {
      int length = readMaybeVInt(bf);
      if (length == Integer.MIN_VALUE) {
         return null;
      } else if (length == 0) {
         return "";
      } else {
         if (!bf.isReadable(length)) {
            bf.resetReaderIndex();
            return null;
         }

         int startIndex = bf.readerIndex();
         bf.skipBytes(length);
         return bf.toString(startIndex, length, StandardCharsets.UTF_8);
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
      if (!bf.isReadable(length)) {
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
