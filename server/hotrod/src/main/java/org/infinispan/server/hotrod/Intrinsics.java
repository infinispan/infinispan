package org.infinispan.server.hotrod;

import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.server.core.transport.ExtendedByteBufJava;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

public class Intrinsics {
   public static int vInt(ByteBuf buf) {
      buf.markReaderIndex();
      return ExtendedByteBufJava.readMaybeVInt(buf);
   }

   public static int signedVInt(ByteBuf buf) {
      buf.markReaderIndex();
      return SignedNumeric.decode(ExtendedByteBufJava.readMaybeVInt(buf));
   }

   public static long vLong(ByteBuf buf) {
      buf.markReaderIndex();
      return ExtendedByteBufJava.readMaybeVLong(buf);
   }

   public static long long_(ByteBuf buf) {
      if (buf.readableBytes() >= 8) {
         return buf.readLong();
      } else return 0;
   }

   public static byte byte_(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte();
      } else return 0;
   }

   public static boolean bool(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte() != 0;
      }
      return false;
   }

   public static byte[] array(ByteBuf buf) {
      buf.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(buf);
   }

   public static byte[] fixedArray(ByteBuf buf, int length) {
      buf.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(buf, length);
   }

   public static String string(ByteBuf buf) {
      buf.markReaderIndex();
      byte[] bytes = ExtendedByteBufJava.readMaybeRangedBytes(buf);
      if (bytes == null) {
         return null;
      } else if (bytes.length == 0) {
         return "";
      } else {
         return new String(bytes, CharsetUtil.UTF_8);
      }
   }

   public static byte[] optionalArray(ByteBuf buf) {
      buf.markReaderIndex();
      int pos = buf.readerIndex();
      int length = ExtendedByteBufJava.readMaybeVInt(buf);
      if (pos == buf.readerIndex()) {
         return null;
      }
      length = SignedNumeric.decode(length);
      if (length < 0) {
         return null;
      }
      return ExtendedByteBufJava.readMaybeRangedBytes(buf, length);
   }

   public static String optionalString(ByteBuf buf) {
      byte[] bytes = optionalArray(buf);
      if (bytes == null) {
         return null;
      } else if (bytes.length == 0) {
         return "";
      } else {
         return new String(bytes, CharsetUtil.UTF_8);
      }
   }

   public static ByteBuf readable(ByteBuf buf, int length) {
      if (buf.readableBytes() >= length) {
         buf.readerIndex(buf.readerIndex() + length);
         return buf;
      } else {
         return null;
      }
   }
}
