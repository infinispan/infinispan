package org.infinispan.commons.netty;

import io.netty.buffer.ByteBuf;

final class BranchlessParser {

   private BranchlessParser() { }

   private static int readableBytes(ByteBuf buffer) {
      return buffer.writerIndex() - buffer.readerIndex();
   }

   private static boolean hasCapacity(ByteBuf buf, int size) {
      return size + buf.readerIndex() <= buf.capacity();
   }

   /**
    * Reads the buffer for an integer value encoded as Varint.
    *
    * <p>
    * This method tries to read the data from the underlying Netty buffer. If there is not enough bytes to decode the
    * number, the method returns <code>0</code>. To distinguish between an actual value, the caller must verify the
    * buffer's reader index. If the index does not change, not data was consumed.
    * </p>
    *
    * <b>Warning:</b> Check the reader index to verify if data was consumed.
    *
    * @param buffer The buffer containing the data.
    * @return The encoded int, or <code>0</code>, if not enough bytes are available.
    */
   public static int readRawVarint32(ByteBuf buffer) {
      if (readableBytes(buffer) < Integer.BYTES)
         return readRawVarint24(buffer);

      int wholeOrMore = buffer.getIntLE(buffer.readerIndex());
      int firstOneOnStop = ~wholeOrMore & 0x80808080;
      if (firstOneOnStop == 0) {
         return readRawVarInt40(buffer, wholeOrMore);
      }
      int bitsToKeep = Integer.numberOfTrailingZeros(firstOneOnStop) + 1;
      buffer.skipBytes(bitsToKeep >> 3);
      int mask = firstOneOnStop ^ (firstOneOnStop - 1);
      return readInt(wholeOrMore & mask);
   }

   private static int readRawVarInt40(ByteBuf buffer, int wholeOrMore) {
      byte lastByte;
      // A number using the whole space. We read the MSB from the last byte and process the remaining 3 bytes to
      // handle the continuation bits.
      if (readableBytes(buffer) <= 4 || (lastByte = buffer.getByte(buffer.readerIndex() + 4)) < 0) {
         buffer.resetReaderIndex();
         return 0;
      }

      buffer.skipBytes(5);
      return lastByte << 28 | readInt(wholeOrMore);
   }

   private static int readInt(int continuation) {
      // mix them up as per varint spec while dropping the continuation bits:
      // 0x7F007F isolate the first byte and the third byte dropping the continuation bits
      // 0x7F007F00 isolate the second byte and the fourth byte dropping the continuation bits
      // the second and fourth byte are shifted to the right by 1, filling the gaps left by the first and third byte
      // it means that the first and second bytes now occupy the first 14 bits (7 bits each)
      // and the third and fourth bytes occupy the next 14 bits (7 bits each), with a gap between the 2s of 2 bytes
      // and another gap of 2 bytes after the forth and third.
      continuation = (continuation & 0x7F007F) | ((continuation & 0x7F007F00) >> 1);
      // 0x3FFF isolate the first 14 bits i.e. the first and second bytes
      // 0x3FFF0000 isolate the next 14 bits i.e. the third and forth bytes
      // the third and forth bytes are shifted to the right by 2, filling the gaps left by the first and second bytes
      return (continuation & 0x3FFF) | ((continuation & 0x3FFF0000) >> 2);
   }

   /**
    * Reads the buffer for a long value encoded as varint.
    *
    * <p>
    * <b>Warning:</b> check the buffer's reader index to verify if data was consumed.
    * </p>
    *
    * @param buffer The buffer containing the encoded value.
    * @return The encoded long, or <code>0</code>, if not enough bytes are available.
    * @see #readRawVarint32(ByteBuf)
    */
   public static long readRawVarint64(ByteBuf buffer) {
      if (readableBytes(buffer) <= Integer.BYTES || !hasCapacity(buffer, Long.BYTES))
         return readRawVarint32(buffer);

      long wholeOrMore = getLongLE(buffer);
      long firstOneOnStop = ~wholeOrMore & 0x8080808080808080L;

      // The value occupies all the bytes. We just unroll it and consume everything.
      if (firstOneOnStop == 0) {
         return readRawVarInt72(buffer, wholeOrMore);
      }

      // Consume the bytes containing the long.
      int bitsToKeep = Long.numberOfTrailingZeros(firstOneOnStop) + 1;
      buffer.skipBytes(bitsToKeep >> 3);

      // Create a mask and create the continuation bytes for decoding.
      long mask = firstOneOnStop ^ (firstOneOnStop - 1);
      return readLong(wholeOrMore & mask);
   }

   private static long getLongLE(ByteBuf buffer) {
      if (buffer instanceof ReplayableByteBuf rbb)
         return getLongLE(rbb.internal());

      return buffer.getLongLE(buffer.readerIndex());
   }

   private static long readRawVarInt72(ByteBuf buffer, long wholeOrMore) {
      byte lastByte;
      int skip = 9;
      long msb;
      // The number occupies all the available space.
      // We have the special case for large negative numbers, where they occupy 9 bytes in varint representation.
      // This way, we have to double-check whether it is a large positive or negative number.
      // During this check, the number of bytes to consume and the MSB representation changes.
      if (readableBytes(buffer) <= 8 || (lastByte = buffer.getByte(buffer.readerIndex() + 8)) < 0) {
         if (readableBytes(buffer) >= 9) {
            if ((lastByte = buffer.getByte(buffer.readerIndex() + 9)) < 0) {
               buffer.resetReaderIndex();
               return 0;
            }

            // A large negative number. We consume all 9 bytes and utilize the last 2 bytes to represent the MSB.
            skip = 10;
            msb = ((long) lastByte << 63) | (((long) buffer.getByte(8) & 0x7F) << 56);
         } else {
            buffer.resetReaderIndex();
            return 0;
         }
      } else {
         // A large positive number.
         // We consume 8 bytes and use only the last byte for the MSB.
         msb = (long) lastByte << 56;
      }

      buffer.skipBytes(skip);
      return msb | readLong(wholeOrMore);
   }

   private static long readLong(long continuation) {
      // We parse it as groups of bytes, first bytes 1, 3, 5, and 7.
      // The second group is 2, 4, 6, and 8, which need a shift to right to compensate the gap.
      continuation = (continuation & 0x007F007F007F007FL) | ((continuation & 0x7F007F007F007F00L) >> 1);

      // Now we isolate the bits in sequence. We check 14 bits at a time.
      // We shift it back to compensate the continuation bits. Since we group two bytes, we shift in increasing steps.
      return (continuation & 0x3FFF) |
            ((continuation & 0x3FFF0000) >> 2) |
            ((continuation & 0x3FFF00000000L) >> 4) |
            ((continuation & 0x3FFF000000000000L) >> 6);
   }

   private static int readRawVarint24(ByteBuf buffer) {
      if (!buffer.isReadable())
         return 0;

      // Reaching this point, we have at most 3 bytes.
      // It is either a smaller number or the buffer still haven't all the necessary bytes to conclude.
      // The number might occupy 1, 2, or 3 bytes. This way, we have to read as much as we can to check.
      int start = buffer.readerIndex();

      // We read the first byte.
      // If it is zero or positive, means we have read the complete value.
      // Otherwise, the number occupies more bytes because the 8 bit is set, marking a continuation.
      byte b = buffer.readByte();
      if (b >= 0)
         return b;

      // The number has the continuation bit set, but we can't read any more bytes.
      // We reset and return.
      if (!buffer.isReadable()) {
         buffer.readerIndex(start);
         return 0;
      }

      // We get the first 7 bits and drop the continuation.
      // And once again, we read the next byte and check if it is zero or positive.
      // Negative numbers means the continuation bit is set and need to continue reading.
      // If the value is positive we have read a 2 bytes number.
      // We keep the first 7 bits and append new byte as msb.
      int result = b & 0x7F;
      if ((b = buffer.readByte()) >= 0)
         return b << 7 | result;

      // If the continuation bit is set, we accumulate the value and continue reading.
      // The number has the format of [<recent 7 bits>, <first 7 bits>].
      result |= (b & 0x7F) << 7;
      if (!buffer.isReadable()) {
         buffer.readerIndex(start);
         return 0;
      }

      // The supposed last byte.
      // This value *must* be positive to identify the end of the number with the last 7 bits missing.
      // If the number is negative, the buffer still haven't received all the necessary bytes to read the complete number.
      if ((b = buffer.readByte()) >= 0)
         return result | b << 14;

      buffer.readerIndex(start);
      return 0;
   }
}
