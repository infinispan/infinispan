package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.commons.io.SignedNumeric.encode;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;

/**
 * Helper methods for writing varints, arrays and strings to {@link ByteBuf}.
 */
public final class ByteBufUtil {
   private ByteBufUtil() {}

   public static byte[] readArray(ByteBuf buf) {
      int length = readVInt(buf);
      byte[] bytes = new byte[length];
      buf.readBytes(bytes, 0, length);
      return bytes;
   }

   public static String readString(ByteBuf buf) {
      byte[] strContent = readArray(buf);
      String readString = new String(strContent, HotRodConstants.HOTROD_STRING_CHARSET);
      return readString;
   }

   public static void writeString(ByteBuf buf, String string) {
      if (string != null && !string.isEmpty()) {
         writeArray(buf, string.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      } else {
         writeVInt(buf, 0);
      }
   }

   public static void writeOptionalString(ByteBuf buf, String string) {
      if (string == null) {
         writeSignedVInt(buf, -1);
      } else {
         writeOptionalArray(buf, string.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      }
   }

   public static void writeArray(ByteBuf buf, byte[] toAppend) {
      writeVInt(buf, toAppend.length);
      buf.writeBytes(toAppend);
   }

   public static void writeArray(ByteBuf buf, byte[] toAppend, int offset, int count) {
      writeVInt(buf, count);
      buf.writeBytes(toAppend, offset, count);
   }

   public static int estimateArraySize(byte[] array) {
      return estimateVIntSize(array.length) + array.length;
   }

   public static int estimateVIntSize(int value) {
      return (32 - Integer.numberOfLeadingZeros(value)) / 7 + 1;
   }

   public static void writeOptionalArray(ByteBuf buf, byte[] toAppend) {
      writeSignedVInt(buf, toAppend.length);
      buf.writeBytes(toAppend);
   }

   public static void writeVInt(ByteBuf buf, int i) {
      while ((i & ~0x7F) != 0) {
         buf.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      buf.writeByte((byte) i);
   }

   public static void writeSignedVInt(ByteBuf buf, int i) {
      writeVInt(buf, encode(i));
   }

   public static void writeVLong(ByteBuf buf, long i) {
      while ((i & ~0x7F) != 0) {
         buf.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      buf.writeByte((byte) i);
   }

   public static int estimateVLongSize(long value) {
      return (64 - Long.numberOfLeadingZeros(value)) / 7 + 1;
   }

   public static long readVLong(ByteBuf buf) {
      byte b = buf.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = buf.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   public static int readVInt(ByteBuf buf) {
      byte b = buf.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = buf.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   public static String hexDump(ByteBuf buf) {
      if (buf.hasArray()) {
         return Util.hexDump(buf.array());
      }
      int currentReaderIndex = buf.readerIndex();
      try {
         buf.resetReaderIndex();
         if (buf.readerIndex() < buf.writerIndex()) {
            byte[] bytes = new byte[Math.min(1024, buf.readableBytes())];
            buf.getBytes(buf.readerIndex(), bytes);
            String dump = Util.hexDump(bytes);
            if (buf.readableBytes() > 1024) {
               dump += "... " + (buf.readableBytes() - 1024) + " more bytes";
            }
            return dump;
         } else {
            return "ri: " + buf.readerIndex() + ", wi: " + buf.writerIndex();
         }
      } finally {
         buf.readerIndex(currentReaderIndex);
      }
   }
}
