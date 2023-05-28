package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.server.resp.commands.CONFIG;
import org.infinispan.server.resp.commands.INFO;
import org.infinispan.server.resp.commands.connection.AUTH;
import org.infinispan.server.resp.commands.connection.COMMAND;
import org.infinispan.server.resp.commands.connection.DBSIZE;
import org.infinispan.server.resp.commands.connection.ECHO;
import org.infinispan.server.resp.commands.connection.HELLO;
import org.infinispan.server.resp.commands.connection.MODULE;
import org.infinispan.server.resp.commands.connection.PING;
import org.infinispan.server.resp.commands.connection.QUIT;
import org.infinispan.server.resp.commands.connection.READONLY;
import org.infinispan.server.resp.commands.connection.READWRITE;
import org.infinispan.server.resp.commands.connection.RESET;
import org.infinispan.server.resp.commands.connection.SELECT;
import org.infinispan.server.resp.commands.list.LPOP;
import org.infinispan.server.resp.commands.list.LPOS;
import org.infinispan.server.resp.commands.list.LSET;
import org.infinispan.server.resp.commands.list.LRANGE;
import org.infinispan.server.resp.commands.list.RPOP;
import org.infinispan.server.resp.commands.list.LINDEX;
import org.infinispan.server.resp.commands.list.LLEN;
import org.infinispan.server.resp.commands.list.LPUSH;
import org.infinispan.server.resp.commands.list.LPUSHX;
import org.infinispan.server.resp.commands.generic.EXISTS;
import org.infinispan.server.resp.commands.list.RPUSH;
import org.infinispan.server.resp.commands.list.RPUSHX;
import org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.PUBLISH;
import org.infinispan.server.resp.commands.pubsub.PUNSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.SUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.UNSUBSCRIBE;
import org.infinispan.server.resp.commands.string.APPEND;
import org.infinispan.server.resp.commands.string.DECR;
import org.infinispan.server.resp.commands.string.DECRBY;
import org.infinispan.server.resp.commands.string.DEL;
import org.infinispan.server.resp.commands.string.GET;
import org.infinispan.server.resp.commands.string.GETDEL;
import org.infinispan.server.resp.commands.string.INCR;
import org.infinispan.server.resp.commands.string.INCRBY;
import org.infinispan.server.resp.commands.string.INCRBYFLOAT;
import org.infinispan.server.resp.commands.string.MGET;
import org.infinispan.server.resp.commands.string.MSET;
import org.infinispan.server.resp.commands.string.SET;
import org.infinispan.server.resp.commands.string.STRLEN;
import org.infinispan.server.resp.commands.string.STRALGO;

import io.netty.buffer.ByteBuf;

public abstract class RespCommand {
   private final String name;
   private final int arity;
   private final int firstKeyPos;
   private final int lastKeyPos;
   private final int steps;
   private final byte[] bytes;

   public RespCommand(int arity, int firstKeyPos, int lastKeyPos, int steps) {
      this.name = this.getClass().getSimpleName();
      this.arity = arity;
      this.firstKeyPos = firstKeyPos;
      this.lastKeyPos = lastKeyPos;
      this.steps = steps;
      this.bytes = name.getBytes(StandardCharsets.US_ASCII);
   }

   public RespCommand(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
      this.name = name;
      this.arity = arity;
      this.firstKeyPos = firstKeyPos;
      this.lastKeyPos = lastKeyPos;
      this.steps = steps;
      this.bytes = name.getBytes(StandardCharsets.US_ASCII);
   }

   protected static List<RespCommand> all() {
      List<RespCommand> respCommands = new ArrayList<>();

      for (int i = 0; i < indexedRespCommand.length; i++) {
         if (indexedRespCommand[i] != null) {
            respCommands.addAll(Arrays.asList(indexedRespCommand[i]));
         }
      }
      return respCommands;
   }

   public String getName() {
      return name;
   }

   private static final RespCommand[][] indexedRespCommand;

   static {
      indexedRespCommand = new RespCommand[26][];
      // Just manual for now, but we may want to dynamically at some point.
      // NOTE that the order within the sub array matters, commands we want to have the lowest latency should be first
      // in this array as they are looked up sequentially for matches
      indexedRespCommand[0] = new RespCommand[]{new APPEND(), new AUTH()};
      indexedRespCommand[2] = new RespCommand[]{new CONFIG(), new COMMAND()};
      // DEL should always be first here
      indexedRespCommand[3] = new RespCommand[]{new DEL(), new DECR(), new DECRBY(), new DBSIZE()};
      indexedRespCommand[4] = new RespCommand[]{new ECHO(), new EXISTS()};
      // GET should always be first here
      indexedRespCommand[6] = new RespCommand[]{new GET(), new GETDEL()};
      indexedRespCommand[7] = new RespCommand[]{new HELLO()};
      indexedRespCommand[8] = new RespCommand[]{new INCR(), new INCRBY(), new INCRBYFLOAT(), new INFO()};
      indexedRespCommand[11] = new RespCommand[]{new LINDEX(), new LPUSH(), new LPUSHX(), new LPOP(), new LRANGE(), new LLEN(), new LPOS(), new LSET() };
      indexedRespCommand[12] = new RespCommand[]{new MGET(), new MSET(), new MODULE()};
      indexedRespCommand[15] = new RespCommand[]{new PUBLISH(), new PING(), new PSUBSCRIBE(), new PUNSUBSCRIBE()};
      indexedRespCommand[16] = new RespCommand[]{new QUIT()};
      indexedRespCommand[17] = new RespCommand[]{new RPUSH(), new RPUSHX(), new RPOP(), new RESET(), new READWRITE(), new READONLY()};
      // SET should always be first here
      indexedRespCommand[18] = new RespCommand[]{new SET(), new STRLEN(), new SUBSCRIBE(), new SELECT(), new STRALGO()};
      indexedRespCommand[20] = new RespCommand[]{new UNSUBSCRIBE()};
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
