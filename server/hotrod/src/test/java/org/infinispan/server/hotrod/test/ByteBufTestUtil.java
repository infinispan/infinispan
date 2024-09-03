package org.infinispan.server.hotrod.test;

import org.infinispan.commons.util.Util;
import org.infinispan.server.core.transport.VInt;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

public final class ByteBufTestUtil {

   private ByteBufTestUtil() { }

   public static int readUnsignedShort(ByteBuf bf) {
      return bf.readUnsignedShort();
   }

   public static int readUnsignedInt(ByteBuf bf) {
      return VInt.read(bf);
   }

   public static long readUnsignedLong(ByteBuf buf) {
      byte b = buf.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = buf.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   public static byte[] readRangedBytes(ByteBuf bf) {
      int length = readUnsignedInt(bf);
      return readRangedBytes(bf, length);
   }

   private static byte[] readRangedBytes(ByteBuf bf, int length) {
      if (length > 0) {
         byte[] array = new byte[length];
         bf.readBytes(array);
         return array;
      } else {
         return Util.EMPTY_BYTE_ARRAY;
      }
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length. If the length is 0, an empty
    * String is returned.
    */
   public static String readString(ByteBuf bf) {
      byte[] bytes = readRangedBytes(bf);
      return bytes.length > 0 ? new String(bytes, CharsetUtil.UTF_8) : "";
   }
}
