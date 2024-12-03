package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.ByteProcessor;

public class Intrinsics {
   private static final int TERMINATOR_LENGTH = 2;

   public static byte singleByte(ByteBuf buffer) {
      if (buffer.isReadable()) {
         return buffer.readByte();
      } else return 0;
   }

   public static String simpleString(ByteBuf buf, int lengthMaximum) {
      // Find the end LF (would be nice to do this without iterating twice)
      int offset = buf.forEachByte(ByteProcessor.FIND_LF);
      if (offset <= 0) {
         assertArrayLength(buf.readableBytes(), lengthMaximum);
         return null;
      }
      if (buf.getByte(offset - 1) != '\r') {
         throw new IllegalStateException("No matching \r character found before \n");
      }
      String simpleString = buf.toString(buf.readerIndex(), offset - 1 - buf.readerIndex(), StandardCharsets.US_ASCII);
      // toString doesn't move the reader index forward
      buf.readerIndex(offset + 1);
      return simpleString;
   }

   public static RespCommand simpleCommand(ByteBuf buf, int maxArraySize) {
      // Find the end LF (would be nice to do this without iterating twice)
      int offset = buf.forEachByte(ByteProcessor.FIND_LF);
      if (offset <= 0) {
         assertArrayLength(buf.readableBytes(), maxArraySize);
         return null;
      }
      if (buf.getByte(offset - 1) != '\r') {
         throw new IllegalStateException("No matching \r character found before \n");
      }
      return RespCommand.fromByteBuf(buf, offset - 1 - buf.readerIndex());
   }

   public static long readNumber(ByteBuf buf, Resp2LongProcessor longProcessor) {
      long value = longProcessor.getValue(buf);
      // The longProcessor can technically read up to only /r, we need to ensure there is /r and /n
      if (longProcessor.complete && buf.readableBytes() >= longProcessor.bytesRead + TERMINATOR_LENGTH) {
         buf.skipBytes(longProcessor.bytesRead + TERMINATOR_LENGTH);
      }
      return value;
   }

   public static byte[] readTerminatedBytes(ByteBuf buf, int maxArraySize) {
      // Find the end LF (would be nice to do this without iterating twice)
      int offset = buf.forEachByte(ByteProcessor.FIND_LF);
      if (offset <= 0) {
         assertArrayLength(buf.readableBytes(), maxArraySize);
         return null;
      }
      if (buf.getByte(offset - 1) != '\r') {
         throw new IllegalStateException("No matching \r character found before \n");
      }
      byte[] data = new byte[offset - 1 - buf.readerIndex()];
      buf.readBytes(data);
      buf.skipBytes(TERMINATOR_LENGTH);
      return data;
   }

   /**
    * This method is only used for numerics that when parsed must be 0 or positive
    * If a valid number is returned, then the ByteBuf will have its read index moved to the next element
    */
   private static int readSizeAndCheckRemainder(ByteBuf buf, Resp2LongProcessor longProcessor) {
      buf.markReaderIndex();
      int pos = buf.readerIndex();
      long longSize = readNumber(buf, longProcessor);
      if (longSize > Integer.MAX_VALUE) {
         throw new IllegalArgumentException("Bytes cannot be longer than " + Integer.MAX_VALUE);
      }
      // If we didn't read any bytes then the number wasn't parsed properly
      if (pos == buf.readerIndex()) {
         return -1;
      }
      int size = (int) longSize;
      if (size < 0) {
         throw new IllegalArgumentException("Number cannot be negative");
      }
      if (buf.readableBytes() < size + TERMINATOR_LENGTH) {
         buf.resetReaderIndex();
         return -1;
      }
      return size;
   }

   private static void assertArrayLength(int length, int lengthMaximum) {
      if (lengthMaximum >= 0 && length > lengthMaximum) {
         throw new TooLongFrameException("Array length " + length + " exceeded " + lengthMaximum);
      }
   }

   public static String bulkString(ByteBuf buf, Resp2LongProcessor longProcessor, int maxArraySize) {
      int size = readSizeAndCheckRemainder(buf, longProcessor);
      if (size == -1) {
         return null;
      }
      assertArrayLength(size, maxArraySize);
      String stringValue = buf.toString(buf.readerIndex(), size, StandardCharsets.US_ASCII);
      buf.skipBytes(size + TERMINATOR_LENGTH);
      return stringValue;
   }

   public static RespCommand bulkCommand(ByteBuf buf, Resp2LongProcessor longProcessor, int maxArraySize) {
      int size = readSizeAndCheckRemainder(buf, longProcessor);
      if (size == -1) {
         return null;
      }
      assertArrayLength(size, maxArraySize);
      return RespCommand.fromByteBuf(buf, size);
   }

   public static byte[] bulkArray(ByteBuf buf, Resp2LongProcessor longProcessor, int maxArraySize) {
      int size = readSizeAndCheckRemainder(buf, longProcessor);
      if (size == -1) {
         return null;
      }
      assertArrayLength(size, maxArraySize);
      byte[] array = new byte[size];
      buf.readBytes(array);
      buf.skipBytes(TERMINATOR_LENGTH);
      return array;
   }

   static class Resp2LongProcessor implements ByteProcessor {
      long result;
      int bytesRead;
      boolean complete;
      boolean negative;
      boolean first;

      public long getValue(ByteBuf buffer) {

         this.result = 0;
         this.bytesRead = 0;
         this.complete = false;
         this.first = true;

         // We didn't have enough to read the number
         if (buffer.forEachByte(this) == -1) {
            complete = false;
            return -1;
         }
         complete = true;

         if (!this.negative) {
            this.result = -this.result;
         }

         return this.result;
      }

      @Override
      public boolean process(byte value) {

         if (value == '\r') {
            return false;
         }

         if (++bytesRead > 8) {
            throw new TooLongFrameException("Long value exceeded 8 bytes");
         }

         if (first) {
            first = false;

            if (value == '-') {
               negative = true;
            } else {
               negative = false;
               int digit = value - '0';
               result = result * 10 - digit;
            }
            return true;
         }

         int digit = value - '0';
         result = result * 10 - digit;

         return true;
      }
   }
}
