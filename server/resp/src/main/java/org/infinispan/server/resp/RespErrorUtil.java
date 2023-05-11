package org.infinispan.server.resp;

public final class RespErrorUtil {

   private RespErrorUtil() {

   }

   public static void wrongType(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERRWRONGTYPE Operation against a key holding the wrong kind of value\r\n", allocatorToUse);

   }
   public static void wrongArgumentNumber(RespCommand command, ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR wrong number of arguments for '" + command.getName().toLowerCase() + "' command\r\n", allocatorToUse);
   }

   public static void mustBePositive(ByteBufPool allocatorToUse) {
      ByteBufferUtils.stringToByteBuf(
            "-ERR value is out of range, must be positive\r\n", allocatorToUse);
   }
}
