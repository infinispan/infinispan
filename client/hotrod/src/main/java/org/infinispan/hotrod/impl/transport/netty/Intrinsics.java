package org.infinispan.hotrod.impl.transport.netty;

import io.netty.buffer.ByteBuf;

public class Intrinsics {
   public static int vInt(ByteBuf buf) {
      return ByteBufUtil.readMaybeVInt(buf);
   }

   public static long vLong(ByteBuf buf) {
      buf.markReaderIndex();
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

   public static long long_(ByteBuf buf) {
      if (buf.readableBytes() >= 8) {
         return buf.readLong();
      }
      return 0;
   }

   public static byte byte_(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte();
      }
      return 0;
   }

   public static short uByte(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readUnsignedByte();
      }
      return 0;
   }

   public static short vShort(ByteBuf buffer) {
      if (buffer.readableBytes() >= Short.BYTES)
         return buffer.readShort();
      return 0;
   }

   public static int uShort(ByteBuf buf) {
      if (buf.readableBytes() >= Short.BYTES)
         return buf.readUnsignedShort();
      return 0;
   }

   public static byte[] array(ByteBuf buf) {
      buf.markReaderIndex();
      return ByteBufUtil.readMaybeArray(buf);
   }

   public static String string(ByteBuf buf) {
      return ByteBufUtil.readString(buf);
   }
}
