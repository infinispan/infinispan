package org.infinispan.server.resp;

import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.infinispan.commons.CacheException;

public final class RespErrorUtil {

   private RespErrorUtil() {
   }

   public static void noSuchKey(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR no such key\r\n", allocatorToUse);
   }

   public static void indexOutOfRange(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR index out of range\r\n", allocatorToUse);

   }

   public static void wrongType(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERRWRONGTYPE Operation against a key holding the wrong kind of value\r\n", allocatorToUse);

   }
   public static void wrongArgumentNumber(RespCommand command, ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command\r\n", allocatorToUse);
   }

   public static void unknownCommand(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBuf("-ERR unknown command\r\n", allocator);
   }

   public static void mustBePositive(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR value is out of range, must be positive\r\n", allocator);
   }

   public static void syntaxError(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBuf("-ERR syntax error\r\n", allocator);
   }

   public static void wrongArgumentCount(RespCommand command, ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBuf("ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command\r\n", allocator);
   }

   public static void valueNotInteger(ByteBufPool allocator) {
      ByteBufferUtils.stringToByteBuf("-ERR value is not an integer or out of range\r\n", allocator);
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

      return null;
   }
}
