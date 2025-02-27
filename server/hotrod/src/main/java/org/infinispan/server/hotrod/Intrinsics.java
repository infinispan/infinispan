package org.infinispan.server.hotrod;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.server.core.transport.ExtendedByteBufJava;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.TooLongFrameException;
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

   public static int int_(ByteBuf buffer) {
      if (buffer.readableBytes() >= 4) {
         return buffer.readInt();
      }
      return 0;
   }

   public static boolean bool(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte() != 0;
      }
      return false;
   }

   private static void assertArrayLength(int length, int lengthMaximum) {
      if (lengthMaximum >= 0 && length > lengthMaximum) {
         throw new TooLongFrameException("Array length " + length + " exceeded " + lengthMaximum);
      }
   }

   public static byte[] array(ByteBuf buf, int lengthMaximum) {
      buf.markReaderIndex();
      int length = ExtendedByteBufJava.readMaybeVInt(buf);
      if (length == Integer.MIN_VALUE) {
         return null;
      }
      assertArrayLength(length, lengthMaximum);
      return ExtendedByteBufJava.readMaybeRangedBytes(buf, length);
   }

   public static byte[] fixedArray(ByteBuf buf, int length, int lengthMaximum) {
      assertArrayLength(length, lengthMaximum);
      buf.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(buf, length);
   }

   public static String string(ByteBuf buf, int lengthMaximum) {
      buf.markReaderIndex();
      int length = ExtendedByteBufJava.readMaybeVInt(buf);
      if (length == Integer.MIN_VALUE) {
         return null;
      } else if (length == 0) {
         return "";
      }
      assertArrayLength(length, lengthMaximum);
      if (!buf.isReadable(length)) {
         buf.resetReaderIndex();
         return null;
      }

      int startIndex = buf.readerIndex();
      buf.skipBytes(length);
      return buf.toString(startIndex, length, StandardCharsets.UTF_8);
   }

   public static byte[] optionalArray(ByteBuf buf, int lengthMaximum) {
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
      assertArrayLength(length, lengthMaximum);
      return ExtendedByteBufJava.readMaybeRangedBytes(buf, length);
   }

   public static String optionalString(ByteBuf buf, int lengthMaximum) {
      byte[] bytes = optionalArray(buf, lengthMaximum);
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

   public static ByteBuf retained(ByteBuf buf, int length) {
      if (buf.readableBytes() >= length) {
         return buf.readRetainedSlice(length);
      } else {
         return null;
      }
   }
}
