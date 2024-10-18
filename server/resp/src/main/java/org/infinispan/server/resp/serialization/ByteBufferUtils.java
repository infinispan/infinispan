package org.infinispan.server.resp.serialization;

import static org.infinispan.server.resp.serialization.RespConstants.CRLF;

import org.infinispan.server.resp.ByteBufPool;

import io.netty.buffer.ByteBuf;

/**
 * Utility class with ByteBuffer Utils
 *
 * @since 14.0
 */
public final class ByteBufferUtils {

   private ByteBufferUtils() {
   }

   static final byte[] DigitTens = {
         '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
         '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
         '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
         '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
         '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
         '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
         '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
         '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
         '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
         '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
   };

   static final byte[] DigitOnes = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
   };

   // This code is a modified version of Integer.toString to write the underlying bytes directly to the ByteBuffer
   // instead of creating a String around a byte[]

   public static int setIntChars(long i, int index, ByteBuf buf) {
      int writeIndex = buf.writerIndex();
      long q;
      int r;
      int charPos = index;

      boolean negative = i < 0;
      if (!negative) {
         i = -i;
      }

      // Generate two digits per iteration
      while (i <= -100) {
         q = i / 100;
         r = (int)((q * 100) - i);
         i = q;
         buf.setByte(writeIndex + --charPos, DigitOnes[r]);
         buf.setByte(writeIndex + --charPos, DigitTens[r]);
      }

      // We know there are at most two digits left at this point.
      q = i / 10;
      r = (int)((q * 10) - i);
      buf.setByte(writeIndex + --charPos, (byte) ('0' + r));

      // Whatever left is the remaining digit.
      if (q < 0) {
         buf.setByte(writeIndex + --charPos, (byte) ('0' - q));
      }

      if (negative) {
         buf.setByte(writeIndex + --charPos, (byte) '-');
      }
      buf.writerIndex(writeIndex + index);
      return charPos;
   }

   public static int stringSize(long x) {
      int d = 1;
      if (x >= 0) {
         d = 0;
         x = -x;
      }
      long p = -10;
      // Iterator up to 18. At 17 the negative value overflows and become positive.
      // At this point, it is the maximum value possible with a long value, it should have
      // 19 (positive) or 20 (negative) digits.
      for (int i = 1; i <= 18; i++) {
         if (x > p)
            return i + d;

         p = 10 * p;
      }
      return 19 + d;
   }

   public static void writeNumericPrefix(byte symbol, long number, ByteBufPool alloc) {
      writeNumericPrefix(symbol, number, alloc, 0);
   }

   public static ByteBuf writeNumericPrefix(byte symbol, long number, ByteBufPool alloc, int additionalWidth) {
      int decimalWidth = ByteBufferUtils.stringSize(number);
      int size = 1 + decimalWidth + 2 + additionalWidth;
      ByteBuf buffer = alloc.acquire(size);
      buffer.writeByte(symbol);
      ByteBufferUtils.setIntChars(number, decimalWidth, buffer);
      buffer.writeBytes(CRLF);
      return buffer;
   }
}
