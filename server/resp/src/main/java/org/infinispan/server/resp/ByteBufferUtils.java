package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;
import static org.infinispan.server.resp.RespConstants.NULL;

import java.util.Collection;

import org.infinispan.multimap.impl.ScoredValue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

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

   public static ByteBuf stringToResult(byte[] result, ByteBufPool alloc) {
      int exactSize = result == null ? 1 + CRLF.length : 1 + stringSize(result.length) + CRLF.length;
      ByteBuf buffer = alloc.acquire(exactSize);
      buffer.writeByte('+');
      if (result!=null) {
         buffer.writeBytes(result);
      }
      buffer.writeBytes(CRLF);
      return buffer;
   }

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
         return alloc.acquire(NULL.length).writeBytes(NULL);
      }
      // : + number of digits + \r\n
      int size = 1 + stringSize(result) + 2;
      ByteBuf buffer = alloc.acquire(size);
      buffer.writeByte(':');
      setIntChars(result, size - 3, buffer);
      return buffer.writeBytes(CRLF);
   }

   public static ByteBuf bytesToResultWrapped(Collection<ScoredValue<byte[]>> results, ByteBufPool alloc) {
      if (results.isEmpty())
         return stringToByteBufAscii("*0\r\n", alloc);

      int resultBytesSize = 0;
      for (ScoredValue<byte[]> result: results) {
         int length;
         if (result == null) {
            // _
            resultBytesSize += 1;
         } else if ((length = result.getValue().length) > 0) {
            // $ + digit length (log10 + 1) + \r\n + byte length
            resultBytesSize += (1 + stringSize(length) + 2 + length);
         } else {
            // $0 + \r\n
            resultBytesSize += (2 + 2);
         }
         // /r/n
         resultBytesSize += 2;
      }
      return bytesToResultWrapped(resultBytesSize, results, alloc);
   }

   public static ByteBuf bytesToResult(Collection<byte[]> results, ByteBufPool alloc) {
      if (results.isEmpty())
         return stringToByteBufAscii("*0\r\n", alloc);

      int resultBytesSize = 0;
      for (byte[] result: results) {
         int length;
         if (result == null) {
            // _
            resultBytesSize += 1;
         } else if ((length = result.length) > 0) {
            // $ + digit length (log10 + 1) + \r\n + byte length
            resultBytesSize += (1 + stringSize(length) + 2 + length);
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
      int elementsSize = stringSize(elements);
      // * + digit length + \r\n + accumulated bytes
      int byteAmount = 1 + elementsSize + 2 + resultBytesSize;
      ByteBuf byteBuf = alloc.apply(byteAmount);
      byteBuf.writeByte('*');
      setIntChars(elements, elementsSize, byteBuf);
      byteBuf.writeBytes(CRLF);
      for (byte[] value : results) {
         if (value == null) {
            byteBuf.writeByte('_');
         } else {
            byteBuf.writeByte('$');
            setIntChars(value.length, stringSize(value.length), byteBuf);
            byteBuf.writeBytes(CRLF);
            byteBuf.writeBytes(value);
         }
         byteBuf.writeBytes(CRLF);
      }
      return byteBuf;
   }

   public static ByteBuf bytesToResultWrapped(int resultBytesSize, Collection<ScoredValue<byte[]>> results, ByteBufPool alloc) {
      int elements = results.size();
      int elementsSize = stringSize(elements);
      // * + digit length + \r\n + accumulated bytes
      int byteAmount = 1 + elementsSize + 2 + resultBytesSize;
      ByteBuf byteBuf = alloc.apply(byteAmount);
      byteBuf.writeByte('*');
      setIntChars(elements, elementsSize, byteBuf);
      byteBuf.writeBytes(CRLF);
      for (ScoredValue<byte[]> scoredValue : results) {
         if (scoredValue == null) {
            byteBuf.writeByte('_');
         } else {
            byteBuf.writeByte('$');
            setIntChars(scoredValue.getValue().length, stringSize(scoredValue.getValue().length), byteBuf);
            byteBuf.writeBytes(CRLF);
            byteBuf.writeBytes(scoredValue.getValue());
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

   public static void writeInt(ByteBuf buf, long value) {
      setIntChars(value, stringSize(value), buf);
   }

   public static ByteBuf stringToByteBufAscii(CharSequence string, ByteBufPool alloc) {
      boolean release = true;
      ByteBuf buffer = alloc.apply(string.length());

      try {
         ByteBufUtil.writeAscii(buffer, string);
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
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
