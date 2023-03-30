package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

public enum RespCommand {
   GET,
   SET,
   INCR,
   DECR,
   DEL,
   MSET,
   MGET,
   PUBLISH,
   PING,
   PSUBSCRIBE,
   PUNSUBSCRIBE,
   SUBSCRIBE,
   UNSUBSCRIBE,
   RESET,
   COMMAND,
   ECHO,
   HELLO,
   AUTH,
   CONFIG,
   INFO,
   READWRITE,
   READONLY,
   SELECT,
   QUIT;

   private final byte[] bytes;

   RespCommand() {
      this.bytes = name().getBytes(StandardCharsets.US_ASCII);
   }

   private static final RespCommand[][] indexedRespCommand;

   static {
      indexedRespCommand = new RespCommand[26][];
      // Just manual for now, but we may want to dynamically do this with ordinal determining what order within
      // a sub array the commands are placed
      indexedRespCommand[0] = new RespCommand[]{AUTH};
      indexedRespCommand[2] = new RespCommand[]{CONFIG, COMMAND};
      indexedRespCommand[3] = new RespCommand[]{DECR, DEL};
      indexedRespCommand[4] = new RespCommand[]{ECHO};
      indexedRespCommand[6] = new RespCommand[]{GET};
      indexedRespCommand[7] = new RespCommand[]{HELLO};
      indexedRespCommand[8] = new RespCommand[]{INCR, INFO};
      indexedRespCommand[12] = new RespCommand[]{MGET, MSET};
      indexedRespCommand[15] = new RespCommand[]{PUBLISH, PING, PSUBSCRIBE, PUNSUBSCRIBE};
      indexedRespCommand[16] = new RespCommand[]{QUIT};
      indexedRespCommand[17] = new RespCommand[]{RESET, READWRITE, READONLY};
      indexedRespCommand[18] = new RespCommand[]{SET, SUBSCRIBE, SELECT};
      indexedRespCommand[20] = new RespCommand[]{UNSUBSCRIBE};
   }

   public static RespCommand fromByteBuf(ByteBuf buf, int commandLength) {
      if (buf.readableBytes() < commandLength + 2) {
         return null;
      }
      int readOffset = buf.readerIndex();
      // We already asserted we have enough bytes, just mark them as read now, since we have to possibly read the
      // bytes multiple times to check for various commands
      buf.readerIndex(readOffset + commandLength + 2);
      byte b = buf.getByte(readOffset);
      byte ignoreCase = b >= 97 ? (byte) (b - 97) : (byte) (b - 65);
      if (ignoreCase < 0 || ignoreCase > 25) {
         return null;
      }
      RespCommand[] target = indexedRespCommand[ignoreCase];
      if (target == null) {
         return null;
      }
      for (RespCommand possible : target) {
         byte[] possibleBytes = possible.bytes;
         if (commandLength == possibleBytes.length) {
            boolean matches = true;
            // Already checked first byte, so skip that one
            for (int i = 1; i < possibleBytes.length; ++i) {
               byte upperByte = possibleBytes[i];
               byte targetByte = buf.getByte(readOffset + i);
               if (upperByte == targetByte || upperByte + 22 == targetByte) {
                  continue;
               }
               matches = false;
               break;
            }
            if (matches) {
               return possible;
            }
         }
      }
      return null;
   }
}
