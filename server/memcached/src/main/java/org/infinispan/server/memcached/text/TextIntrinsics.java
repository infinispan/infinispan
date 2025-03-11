package org.infinispan.server.memcached.text;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.infinispan.server.core.transport.ExtendedByteBufJava;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.Signal;

public class TextIntrinsics {
   // [0-9]
   private static final BitSet NUMBERS = BitSet.valueOf(new long[]{287948901175001088L});
   // [A-Za-z_]
   private static final BitSet LETTERS = BitSet.valueOf(new long[]{0, 576460745995190270L});
   // [^\n\r ]
   private static final BitSet TEXT = BitSet.valueOf(new long[]{-4294976513L, -1, -1, -1});

   public static short short_(ByteBuf b) {
      if (b.readableBytes() >= 2) {
         return b.readShort();
      } else return 0;
   }

   public static byte[] fixedArray(ByteBuf b, int length, int maxArrayLength) {
      if (maxArrayLength > 0 && length > maxArrayLength) {
         throw new TooLongFrameException("Array length " + length + " exceeded " + maxArrayLength);
      }
      b.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(b, length);
   }

   private static boolean consumeLine(ByteBuf buf) {
      int index = buf.readerIndex();
      while (buf.isReadable()) {
         byte b = buf.readByte();
         if (b == 10) {
            return true;
         }
      }
      buf.readerIndex(index);
      return false;
   }

   private static int readBuf(ByteBuf buf, TokenReader reader) {
      try {
         return buf.forEachByte(reader);
      } catch (Signal s) {
         if (!consumeLine(buf)) {
            return 0;
         }

         throw s;
      }
   }

   private static ByteBuf token(ByteBuf buf, TokenReader reader) {
      int offset = readBuf(buf, reader);
      if (offset <= 0) return null;

      try {
         return reader.output();
      } finally {
         buf.skipBytes(reader.readBytesSize());
      }
   }

   public static long long_number(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      int index = buf.readerIndex();
      ByteBuf s = token(buf, reader.forToken(NUMBERS, bytesAvailable));
      if (s == null) {
         return 0;
      } else if (s.isReadable()) {
         return Long.parseUnsignedLong(s.toString(US_ASCII));
      } else {
         if (!consumeLine(buf)) {
            buf.readerIndex(index);
            return 0;
         }
         throw TokenReader.INVALID_TOKEN;
      }
   }

   public static int int_number(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      int index = buf.readerIndex();
      ByteBuf s = token(buf, reader.forToken(NUMBERS, bytesAvailable));
      if (s == null) {
         return 0;
      } else if (s.isReadable()) {
         return parseUnsignedInt(s);
      } else {
         if (!consumeLine(buf)) {
            buf.readerIndex(index);
            return 0;
         }

         throw TokenReader.INVALID_TOKEN;
      }
   }

   public static TextCommand command(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      int index = buf.readerIndex();
      ByteBuf id = token(buf, reader.forToken(LETTERS, bytesAvailable));
      try {
         return id == null ? null : TextCommand.valueOf(id);
      } catch (IllegalArgumentException e) {
         if (!consumeLine(buf)) {
            buf.readerIndex(index);
            return null;
         }
         throw new UnsupportedOperationException(id.toString(US_ASCII));
      }
   }

   public static byte[] text(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      ByteBuf s = token(buf, reader.forToken(TEXT, bytesAvailable));
      if (s == null || !s.isReadable()) {
         return null;
      } else {
         byte[] b = new byte[s.readableBytes()];
         s.readBytes(b);
         return b;
      }
   }

   public static byte[] text_key(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      int index = buf.readerIndex();
      ByteBuf s = token(buf, reader.forToken(TEXT, bytesAvailable));
      if (s == null || !s.isReadable()) {
         return null;
      } else if (s.readableBytes() > 250) {
         if (!consumeLine(buf)) {
            buf.readerIndex(index);
            return null;
         }
         throw new IllegalArgumentException("Key length over the 250 character limit");
      } else {
         byte[] b = new byte[s.readableBytes()];
         s.readBytes(b);
         return b;
      }
   }

   public static List<byte[]> text_list(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      return readByteList(buf, reader, bytesAvailable);
   }

   public static List<byte[]> text_key_list(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      return readByteList(buf, reader, bytesAvailable);
   }

   private static List<byte[]> readByteList(ByteBuf buf, TokenReader reader, int bytesAvailable) {
      buf.markReaderIndex();
      List<byte[]> list = new ArrayList<>();
      while (true) {
         int r = buf.readerIndex();
         byte[] b = text_key(buf, reader, bytesAvailable);
         if (b == null) {
            // If element is null and still same index on reader, the buffer is not complete to read the next element.
            if (buf.readerIndex() == r) {
               buf.resetReaderIndex();
               return Collections.emptyList();
            }
            break;
         }

         list.add(b);
      }
      return list;
   }

   public static boolean eowc(ByteBuf buf) {
      buf.markReaderIndex();
      if (buf.readableBytes() > 1) {
         short b = buf.readUnsignedByte();
         if (b == 13) {
            buf.readByte(); // eat the LF
            return false;
         } else if (b == TextConstants.NOREPLY[0]) {
            int pos = 1;
            while (buf.readableBytes() > 0) {
               b = buf.readUnsignedByte();
               if (b != TextConstants.NOREPLY[pos]) {
                  if (!consumeLine(buf)) {
                     buf.resetReaderIndex();
                     return false;
                  }
                  throw TokenReader.INVALID_TOKEN;
               }
               if (++pos == TextConstants.NOREPLY.length) {
                  return true;
               }
            }
         } else {
            if (!consumeLine(buf)) {
               buf.resetReaderIndex();
               return false;
            }

            throw TokenReader.INVALID_TOKEN;
         }
      }
      buf.resetReaderIndex();
      return false;
   }

   public static long parseLong(ByteBuf s) {
      byte first = s.readByte();
      long result = first == '+' ? 0 : first - 48;
      while (s.isReadable()) {
         byte b = s.readByte();
         if (b < '0' || b > '9')
            throw new NumberFormatException("Invalid character: " + b);

         result = (result << 3) + (result << 1) + (b - 48);
      }
      return result;
   }

   public static int parseUnsignedInt(ByteBuf s) {
      long v = parseLong(s);
      // From Integer.parseUnsignedInt.
      if ((v & 0xffff_ffff_0000_0000L) != 0L) {
         throw new NumberFormatException("Value exceeds range of unsigned int: " + v);
      }
      return (int)v;
   }
}
