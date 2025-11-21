package org.infinispan.server.resp;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import javax.security.sasl.SaslException;

import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.Resp3Response;

public final class RespErrorUtil {

   private RespErrorUtil() {
   }

   public static void unauthorized(ByteBufPool allocator) {
      simpleErrorResponse("-WRONGPASS invalid username-password pair or user is disabled.",
            allocator);
   }

   public static void noSuchKey(ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-ERR no such key", allocatorToUse);
   }

   public static void indexOutOfRange(ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-ERR index out of range", allocatorToUse);

   }

   public static void wrongType(ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-WRONGTYPE Operation against a key holding the wrong kind of value", allocatorToUse);

   }

   public static void wrongArgumentNumber(RespCommand command, ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command", allocatorToUse);
   }

   public static void unknownCommand(ByteBufPool allocator) {
      simpleErrorResponse("-ERR unknown command", allocator);
   }

   public static void mustBePositive(ByteBufPool allocator) {
      simpleErrorResponse("-ERR value is out of range, must be positive", allocator);
   }

   public static void mustBePositive(ByteBufPool allocator, String argumentName) {
      simpleErrorResponse(
            "-ERR value for ' " + argumentName + "' is out of range, must be positive", allocator);
   }

   public static void syntaxError(ByteBufPool allocator) {
      simpleErrorResponse("-ERR syntax error", allocator);
   }

   public static void wrongArgumentCount(RespCommand command, ByteBufPool allocator) {
      simpleErrorResponse(
            "ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command", allocator);
   }

   public static void valueNotInteger(ByteBufPool allocator) {
      simpleErrorResponse("-ERR value is not an integer or out of range", allocator);
   }

   public static void valueNotAValidFloat(ByteBufPool allocator) {
      simpleErrorResponse("-ERR value is not a valid float", allocator);
   }

   public static void minOrMaxNotAValidFloat(ByteBufPool allocator) {
      simpleErrorResponse("-ERR min or max is not a float", allocator);
   }

   public static void nanOrInfinity(ByteBufPool allocator) {
      simpleErrorResponse("-ERR increment would produce NaN or Infinity", allocator);
   }

   public static void minOrMaxNotAValidStringRange(ByteBufPool allocator) {
      simpleErrorResponse("-ERR min or max not valid string range item", allocator);
   }

   public static void transactionAborted(ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-EXECABORT Transaction discarded because of previous errors.", allocatorToUse);
   }

   public static void customError(String message, ByteBufPool allocatorToUse) {
      simpleErrorResponse(
            "-ERR " + message, allocatorToUse);
   }

   public static void customRawError(String message, ByteBufPool alloc) {
      simpleErrorResponse(message, alloc);
   }

   public static Consumer<ByteBufPool> handleException(Throwable t) {
      Throwable ex = t;
      while (ex instanceof CompletionException || ex instanceof CacheException) {
         ex = ex.getCause();
      }

      if (ex instanceof RespCommandException rce) {
         return bbp -> Resp3Response.error(String.format("-%s", rce.getMessage()), bbp);
      }

      if (ex instanceof ClassCastException) {
         return RespErrorUtil::wrongType;
      }

      if (ex instanceof IllegalArgumentException) {
         if (ex.getMessage().contains("No marshaller registered for object of Java type")) {
            return RespErrorUtil::wrongType;
         }
      }

      if (ex instanceof IndexOutOfBoundsException) {
         return RespErrorUtil::indexOutOfRange;
      }

      if (ex instanceof NumberFormatException) {
         return RespErrorUtil::valueNotInteger;
      }

      if (ex instanceof SecurityException || ex instanceof SaslException) {
         return RespErrorUtil::unauthorized;
      }

      return null;
   }

   public static boolean isWrongTypeError(Throwable t) {
      while (t instanceof CompletionException || t instanceof CacheException || t instanceof ExecutionException) {
         t = t.getCause();
      }
      return t instanceof ClassCastException ||
            (t instanceof IllegalArgumentException &&
                  t.getMessage().contains("No marshaller registered for object of Java type"));
   }

   private static void simpleErrorResponse(CharSequence string, ByteBufPool alloc) {
      Resp3Response.error(string, alloc);
   }
}
