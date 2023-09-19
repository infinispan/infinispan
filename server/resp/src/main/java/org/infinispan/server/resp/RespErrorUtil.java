package org.infinispan.server.resp;

import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.infinispan.commons.CacheException;

public final class RespErrorUtil {

   private RespErrorUtil() {
   }

   public static void noSuchKey(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBufAscii(
            "-ERR no such key\r\n", allocatorToUse);
   }

   public static void indexOutOfRange(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBufAscii(
            "-ERR index out of range\r\n", allocatorToUse);

   }

   public static void wrongType(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBufAscii(
            "-ERRWRONGTYPE Operation against a key holding the wrong kind of value\r\n", allocatorToUse);

   }
   public static void wrongArgumentNumber(RespCommand command, ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBufAscii(
            "-ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command\r\n", allocatorToUse);
   }

   public static void unknownCommand(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR unknown command\r\n", allocator);
   }

   public static void mustBePositive(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii(
            "-ERR value is out of range, must be positive\r\n", allocator);
   }

   public static void syntaxError(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR syntax error\r\n", allocator);
   }

   public static void wrongArgumentCount(RespCommand command, ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command\r\n", allocator);
   }

   public static void valueNotInteger(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR value is not an integer or out of range\r\n", allocator);
   }

   public static void valueNotAValidFloat(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR value is not a valid float\r\n", allocator);
   }

   public static void minOrMaxNotAValidFloat(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR min or max is not a float\r\n", allocator);
   }

   public static void minOrMaxNotAValidStringRange(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBufAscii("-ERR min or max not valid string range item\r\n", allocator);
   }

   public static void transactionAborted(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBufAscii(
            "-EXECABORT Transaction discarded because of previous errors.\r\n", allocatorToUse);
   }

   public static void customError(String message, ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR " + message + "\r\n", allocatorToUse);
   }

   public static Consumer<ByteBufPool> handleException(Throwable t) {
      Throwable ex = t;
      while (ex instanceof CompletionException || ex instanceof CacheException) {
         ex = ex.getCause();
      }

      if (ex instanceof ClassCastException) {
         return RespErrorUtil::wrongType;
      }

      if (ex instanceof IndexOutOfBoundsException) {
         return RespErrorUtil::indexOutOfRange;
      }

      if (ex instanceof NumberFormatException) {
         return RespErrorUtil::valueNotInteger;
      }

      return null;
   }
}
