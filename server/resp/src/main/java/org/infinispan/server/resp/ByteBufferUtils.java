package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;
import static org.infinispan.server.resp.RespConstants.NIL;

import java.util.Collection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.CharsetUtil;

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

   public static ByteBuf bytesToResult(byte[] result, ByteBufPool alloc) {
      int length = result.length;
      int stringLength = stringSize(length);

      // Need 5 extra for $ and 2 sets of /r/n
      int exactSize = stringLength + length + 5;
      ByteBuf buffer = alloc.acquire(exactSize);
      buffer.writeByte('$');
      // This method is anywhere from 10-100% faster than ByteBufUtil.writeAscii and avoids allocations
      setIntChars(length, stringLength, buffer);
      buffer.writeBytes(CRLF);
      buffer.writeBytes(result);
      buffer.writeBytes(CRLF);

      return buffer;
   }

   public static ByteBuf writeLong(Long result, ByteBufPool alloc) {
      if (result == null) {
         return alloc.acquire(NIL.length).writeBytes(NIL);
      }
      // : + number of digits + \r\n
      int size = 1 + stringSize(result) + 2;
      ByteBuf buffer = alloc.acquire(size);
      buffer.writeByte(':');
      setIntChars(result, size - 3, buffer);
      return buffer.writeBytes(CRLF);
   }

   public static ByteBuf bytesToResult(Collection<byte[]> results, ByteBufPool alloc) {
      if (results.isEmpty())
         return stringToByteBuf("*0\r\n", alloc);

      int resultBytesSize = 0;
      for (byte[] result: results) {
         int length = result.length;
         if (length > 0) {
            // $ + digit length (log10 + 1) + \r\n + byte length
            resultBytesSize += (1 + (int) Math.log10(length) + 1 + 2 + length);
         } else {
            // $0 + \r\n
            resultBytesSize += (2 + 2);
         }
         // /r/n
         resultBytesSize += 2;
      }
      return bytesToResult(resultBytesSize, results, alloc);
   }

   public static ByteBuf bytesToResult(int resultBytesSize, Collection<byte[]> results, ByteBufPool alloc) {
      int elements = results.size();
      // * + digit length (log10 + 1) + \r\n + accumulated bytes
      int byteAmount = 1 + (int) Math.log10(elements) + 1 + 2 + resultBytesSize;
      ByteBuf byteBuf = alloc.apply(byteAmount);
      byteBuf.writeCharSequence("*" + results.size(), CharsetUtil.US_ASCII);
      byteBuf.writeBytes(CRLF);
      for (byte[] value : results) {
         if (value == null) {
            byteBuf.writeCharSequence("$-1", CharsetUtil.US_ASCII);
         } else {
            byteBuf.writeCharSequence("$" + value.length, CharsetUtil.US_ASCII);
            byteBuf.writeBytes(CRLF);
            byteBuf.writeBytes(value);
         }
         byteBuf.writeBytes(CRLF);
      }
      return byteBuf;
   }

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
      int p = -10;
      for (int i = 1; i < 10; i++) {
         if (x > p)
            return i + d;
         p = 10 * p;
      }
      return 10 + d;
   }


   public static ByteBuf stringToByteBuf(CharSequence string, ByteBufPool alloc) {
      return stringToByteBufWithExtra(string, alloc, 0);
   }

   public static ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufPool alloc, int extraBytes) {
      boolean release = true;
      int stringBytes = ByteBufUtil.utf8Bytes(string);
      int allocatedSize = stringBytes + extraBytes;
      ByteBuf buffer = alloc.apply(allocatedSize);

      try {
         int beforeWriteIndex = buffer.writerIndex();
         ByteBufUtil.reserveAndWriteUtf8(buffer, string, allocatedSize);
         assert buffer.capacity() - buffer.writerIndex() >= extraBytes;
         assert buffer.writerIndex() - beforeWriteIndex == stringBytes;
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
   }
}
