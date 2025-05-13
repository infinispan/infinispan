package org.infinispan.server.resp;

import static org.infinispan.server.resp.commands.Commands.ALL_COMMANDS;
import static org.infinispan.server.resp.serialization.RespConstants.CRLF;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.commands.BaseResp3Command;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.bytebuf.ByteBufferUtils;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public abstract class RespCommand implements BaseResp3Command {
   protected static final Log log = LogFactory.getLog(RespCommand.class, Log.class);
   private final String name;
   private final int arity;
   private final int firstKeyPos;
   private final int lastKeyPos;
   private final int steps;
   private final byte[] bytes;
   private final long aclMask;

   public boolean hasValidNumberOfArguments(List<byte[]> arguments){
      // Command arity always includes the command's name itself (and the subcommand when applicable).
      // A positive integer means a fixed number of arguments.
      // A negative integer means a minimal number of arguments.
      int numberOfArgs = Math.abs(arity) - 1;
      // ERROR
      return (arity <= 0 || arguments.size() == numberOfArgs)
            && (arity >= 0 || arguments.size() >= numberOfArgs);
   }

   public CompletionStage<RespRequestHandler> handleException(RespRequestHandler handler, Throwable t) {
      Consumer<ResponseWriter> writer = ResponseWriter.handleException(t);
      if (writer != null) {
         writer.accept(handler.writer);
         return handler.myStage();
      }

      throw CompletableFutures.asCompletionException(t);
   }

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
   protected RespCommand(int arity, int firstKeyPos, int lastKeyPos, int steps, long aclMask) {
      this.name = this.getClass().getSimpleName();
      this.arity = arity;
      this.firstKeyPos = firstKeyPos;
      this.lastKeyPos = lastKeyPos;
      this.steps = steps;
      this.bytes = name.getBytes(StandardCharsets.US_ASCII);
      this.aclMask = aclMask;
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
   protected RespCommand(String name, int arity, int firstKeyPos, int lastKeyPos, int steps, long aclMask) {
      this.name = name;
      this.arity = arity;
      this.firstKeyPos = firstKeyPos;
      this.lastKeyPos = lastKeyPos;
      this.steps = steps;
      this.bytes = name.getBytes(StandardCharsets.US_ASCII);
      this.aclMask = aclMask;
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
         if (possible.match(buf, commandLength, readOffset)) {
            return possible;
         }
      }
      return null;
   }

   public static RespCommand fromString(String s) {
      char c0 = s.charAt(0);
      byte ignoreCase = c0 >= 97 ? (byte) (c0 - 97) : (byte) (c0 - 65);
      if (ignoreCase < 0 || ignoreCase > 25) {
         return null;
      }
      RespCommand[] target = ALL_COMMANDS[ignoreCase];
      if (target == null) {
         return null;
      }
      for (RespCommand possible : target) {
         if (RespUtil.isAsciiBytesEquals(possible.bytes, s)) {
            return possible;
         }
      }
      return null;
   }

   public final boolean match(byte[] other) {
      return match(Unpooled.wrappedBuffer(other), other.length, 0);
   }

   private boolean match(ByteBuf buf, int length, int offset) {
      byte[] possibleBytes = bytes;
      if (length == possibleBytes.length) {
         boolean matches = true;
         for (int i = 0; i < possibleBytes.length; ++i) {
            byte upperByte = possibleBytes[i];
            byte targetByte = buf.getByte(offset + i);
            if (upperByte == targetByte || upperByte + 32 == targetByte) {
               continue;
            }
            matches = false;
            break;
         }
         return matches;
      }
      log.tracef("Unknown command %s", buf.getCharSequence(offset, length, StandardCharsets.US_ASCII));
      return false;
   }

   public int size(List<byte[]> arguments) {
      // The command has a fixed prefix + the name itself.
      // The type and the number of leading command/arguments.
      int base = 1 + ByteBufferUtils.stringSize(arguments.size()) + CRLF.length;

      int argSize = 0;
      for (byte[] argument : arguments) {
         // The argument type identifier + the actual argument contents + the line break.
         argSize += 1 + argument.length + CRLF.length;
      }

      // The command name can be provided as a bulk string ($4\r\nHELLO\r\n) or simple string (+HELLO\r\n).
      // At this points we cant differentiate, so lets go with the worst case.
      int nameSize = 1 + ByteBufferUtils.stringSize(name.length()) + CRLF.length + name.length() + CRLF.length;

      return base + nameSize + argSize;
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

   public final long aclMask() {
      return aclMask;
   }

   public byte[][] extractKeys(List<byte[]> arguments) {
      // Position 0 is the command's name. This means no keys are present.
      if (getFirstKeyPos() == 0) {
         return Util.EMPTY_BYTE_ARRAY_ARRAY;
      }

      List<byte[]> keys = new ArrayList<>();

      // Last key pos negative means unbounded number of keys.
      int end = getLastKeyPos() < 0 ? arguments.size() : getLastKeyPos();
      for (int i = getFirstKeyPos() - 1; i < end; i += getSteps()) {
         keys.add(arguments.get(i));
      }
      return keys.toArray(Util.EMPTY_BYTE_ARRAY_ARRAY);
   }

   @Override
   public String toString() {
      return name;
   }
}
