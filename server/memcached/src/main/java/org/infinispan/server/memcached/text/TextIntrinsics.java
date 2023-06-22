package org.infinispan.server.memcached.text;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.infinispan.server.core.transport.ExtendedByteBufJava;

import io.netty.buffer.ByteBuf;

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

   public static byte[] fixedArray(ByteBuf b, int length) {
      b.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(b, length);
   }

   private static Void consumeLine(ByteBuf buf) {
      buf.markReaderIndex();
      while (buf.isReadable()) {
         byte b = buf.readByte();
         if (b == 10) {
            return null;
         }
      }
      buf.resetReaderIndex();
      return null;
   }

   private static ByteBuf token(ByteBuf buf, TokenReader reader) {
      int offset = buf.forEachByte(reader);
      if (offset <= 0) return null;

      try {
         return reader.output();
      } finally {
         buf.skipBytes(reader.readBytesSize());
      }
   }

   public static long long_number(ByteBuf buf, TokenReader reader) {
      ByteBuf s = token(buf, reader.forToken(NUMBERS));
      if (s == null) {
         return 0;
      } else if (s.isReadable()) {
         return Long.parseUnsignedLong(s.toString(US_ASCII));
      } else {
         consumeLine(buf);
         throw new IllegalArgumentException("Expected number");
      }
   }

   public static int int_number(ByteBuf buf, TokenReader reader) {
      ByteBuf s = token(buf, reader.forToken(NUMBERS));
      if (s == null) {
         return 0;
      } else if (s.isReadable()) {
         return parseUnsignedInt(s);
      } else {
         consumeLine(buf);
         throw new IllegalArgumentException("Expected number");
      }
   }

   public static TextCommand command(ByteBuf buf, TokenReader reader) {
      ByteBuf id = token(buf, reader.forToken(LETTERS));
      try {
         return id == null ? null : TextCommand.valueOf(id);
      } catch (IllegalArgumentException e) {
         consumeLine(buf);
         throw new UnsupportedOperationException(id.toString(US_ASCII));
      }
   }

   public static byte[] text(ByteBuf buf, TokenReader reader) {
      ByteBuf s = token(buf, reader.forToken(TEXT));
      if (s == null || !s.isReadable()) {
         return null;
      } else {
         byte[] b = new byte[s.readableBytes()];
         s.readBytes(b);
         return b;
      }
   }

   public static byte[] text_key(ByteBuf buf, TokenReader reader) {
      ByteBuf s = token(buf, reader.forToken(TEXT));
      if (s == null || !s.isReadable()) {
         return null;
      } else if (s.readableBytes() > 250) {
         consumeLine(buf);
         throw new IllegalArgumentException("Key length over the 250 character limit");
      } else {
         byte[] b = new byte[s.readableBytes()];
         s.readBytes(b);
         return b;
      }
   }

   public static List<byte[]> text_list(ByteBuf buf, TokenReader reader) {
      List<byte[]> list = new ArrayList<>();
      for (byte[] b = text(buf, reader); b != null; b = text(buf, reader)) {
         list.add(b);
      }
      return list;
   }

   public static List<byte[]> text_key_list(ByteBuf buf, TokenReader reader) {
      List<byte[]> list = new ArrayList<>();
      for (byte[] b = text_key(buf, reader); b != null; b = text_key(buf, reader)) {
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
                  consumeLine(buf);
                  throw new IllegalArgumentException();
               }
               if (++pos == TextConstants.NOREPLY.length) {
                  return true;
               }
            }
         } else {
            consumeLine(buf);
            throw new IllegalArgumentException();
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
