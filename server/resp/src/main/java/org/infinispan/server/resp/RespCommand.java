package org.infinispan.server.resp;

import static org.infinispan.server.resp.commands.Commands.ALL_COMMANDS;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;

public abstract class RespCommand {
   protected final static Log log = LogFactory.getLog(RespCommand.class, Log.class);
   private final String name;
   private final int arity;
   private final int firstKeyPos;
   private final int lastKeyPos;
   private final int steps;
   private final byte[] bytes;


   /**
    * @param arity       the number of arguments
    * @param firstKeyPos the position of the command's first key name argument. For most commands, the first key's position is 1.
    *                    Position 0 is always the command name itself.
    * @param lastKeyPos  the position of the command's last key name argument. Redis commands usually accept one, two or multiple number of keys.
    *                    Commands that accept a single key have both first key and last key set to 1.
    *                    Commands that accept two key name arguments, e.g. BRPOPLPUSH, SMOVE and RENAME, have this value set to the position of their second key.
    *                    Multi-key commands that accept an arbitrary number of keys, such as MSET, use the value -1.
    * @param steps       the step, or increment, between the first key and the position of the next key.
    */
   protected RespCommand(int arity, int firstKeyPos, int lastKeyPos, int steps) {
      this(MethodHandles.lookup().getClass().getSimpleName(), arity, firstKeyPos, lastKeyPos, steps);
   }

   /**
    * @param name        the name of the command
    * @param arity       the number of arguments
    * @param firstKeyPos the position of the command's first key name argument. For most commands, the first key's position is 1.
    *                    Position 0 is always the command name itself.
    * @param lastKeyPos  the position of the command's last key name argument. Redis commands usually accept one, two or multiple number of keys.
    *                    Commands that accept a single key have both first key and last key set to 1.
    *                    Commands that accept two key name arguments, e.g. BRPOPLPUSH, SMOVE and RENAME, have this value set to the position of their second key.
    *                    Multi-key commands that accept an arbitrary number of keys, such as MSET, use the value -1.
    * @param steps       the step, or increment, between the first key and the position of the next key.
    */
   protected RespCommand(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
      this.name = name;
      this.arity = arity;
      this.firstKeyPos = firstKeyPos;
      this.lastKeyPos = lastKeyPos;
      this.steps = steps;
      this.bytes = name.getBytes(StandardCharsets.US_ASCII);
   }

   public String getName() {
      return name;
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
      RespCommand[] target = ALL_COMMANDS[ignoreCase];
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
               if (upperByte == targetByte || upperByte + 32 == targetByte) {
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
      log.tracef("Unknown command %s", buf.getCharSequence(readOffset, commandLength, StandardCharsets.US_ASCII));
      return null;
   }

   public int getArity() {
      return arity;
   }

   public int getFirstKeyPos() {
      return firstKeyPos;
   }

   public int getLastKeyPos() {
      return lastKeyPos;
   }

   public int getSteps() {
      return steps;
   }
}
