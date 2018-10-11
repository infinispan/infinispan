package org.infinispan.server.core.transport;

import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 * Static helper class that provides methods to be used with a {@link ByteBuf} that are useful for Infinispan.
 */
public class ExtendedByteBuf {
   private ExtendedByteBuf() { }

   public static ByteBuf wrappedBuffer(byte[]... arrays) {
      return Unpooled.wrappedBuffer(arrays);
   }

   public static ByteBuf buffer(int capacity) {
      return Unpooled.buffer(capacity);
   }

   public static ByteBuf dynamicBuffer() {
      return Unpooled.buffer();
   }

   public static int readUnsignedShort(ByteBuf bf) {
      return bf.readUnsignedShort();
   }

   public static int readUnsignedInt(ByteBuf bf) {
      return VInt.read(bf);
   }

   public static long readUnsignedLong(ByteBuf bf) {
      return VLong.read(bf);
   }

   public static byte[] readRangedBytes(ByteBuf bf) {
      int length = readUnsignedInt(bf);
      return readRangedBytes(bf, length);
   }

   public static byte[] readRangedBytes(ByteBuf bf, int length) {
      if (length > 0) {
         byte[] array = new byte[length];
         bf.readBytes(array);
         return array;
      } else {
         return Util.EMPTY_BYTE_ARRAY;
      }
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    * If the length is 0, an empty String is returned.
    */
   public static String readString(ByteBuf bf) {
      byte[] bytes = readRangedBytes(bf);
      return bytes.length > 0 ? new String(bytes, CharsetUtil.UTF_8) : "";
   }

   public static void writeUnsignedShort(int i, ByteBuf bf) {
      bf.writeShort(i);
   }

   public static void writeUnsignedInt(int i, ByteBuf bf) {
      VInt.write(bf, i);
   }

   public static void writeUnsignedLong(long l, ByteBuf bf) {
      VLong.write(bf, l);
   }

   public static void writeRangedBytes(byte[] src, ByteBuf bf) {
      writeUnsignedInt(src.length, bf);
      if (src.length > 0)
         bf.writeBytes(src);
   }

   public static void writeString(String msg, ByteBuf bf) {
      writeRangedBytes(msg.getBytes(CharsetUtil.UTF_8), bf);
   }
}
