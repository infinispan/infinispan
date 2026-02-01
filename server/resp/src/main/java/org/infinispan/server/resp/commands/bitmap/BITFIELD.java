package org.infinispan.server.resp.commands.bitmap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * Executes the Redis BITFIELD command with its multiple subcommands.
 * The BITFIELD command is a versatile tool for manipulating bitfields stored in Redis strings.
 * It allows for setting, getting, and incrementing arbitrary-width signed and unsigned integers
 * at any position within the string.
 * This implementation supports the GET, SET, and INCRBY subcommands, along with the OVERFLOW
 * option for fine-grained control over increment and set operations that exceed the specified
 * integer size.
 *
 * @see <a href="https://redis.io/commands/bitfield/">Redis BITFIELD command</a>
 * @since 16.2
 */
public class BITFIELD extends RespCommand implements Resp3Command {
   protected final boolean readOnly;

   public BITFIELD() {
      this(false, AclCategory.BITMAP.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
   }

   protected BITFIELD(boolean readOnly, long mask) {
      super(-4, 1, 1, 1, mask);
      this.readOnly = readOnly;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<BitfieldOperation> operations = new ArrayList<>();
      BitfieldOperation.Overflow overflow = BitfieldOperation.Overflow.NONE;

      for (int i = 1; i < arguments.size(); i++) {
         String subcommand = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         switch (subcommand) {
            case "GET":
               operations.add(BitfieldOperation.GET(arguments.get(++i), arguments.get(++i)));
               break;
            case "SET":
               if (readOnly) {
                  throw new IllegalArgumentException("ERR syntax error");
               }
               operations.add(BitfieldOperation.SET(arguments.get(++i), arguments.get(++i), arguments.get(++i), overflow));
               break;
            case "INCRBY":
               if (readOnly) {
                  throw new IllegalArgumentException("ERR syntax error");
               }
               operations.add(BitfieldOperation.INCRBY(arguments.get(++i), arguments.get(++i), arguments.get(++i), overflow));
               break;
            case "OVERFLOW":
               if (readOnly) {
                  throw new IllegalArgumentException("ERR syntax error");
               }
               overflow = BitfieldOperation.Overflow.valueOf(new String(arguments.get(++i), StandardCharsets.US_ASCII).toUpperCase());
               break;
            default:
               throw new IllegalArgumentException("ERR syntax error");
         }
      }
      FunctionalMap.ReadWriteMap<byte[], byte[]> fMap = FunctionalMap.create(handler.cache()).toReadWriteMap();
      CompletableFuture<List<Long>> results = fMap.eval(key, new BitfieldFunction(operations));
      return handler.stageToReturn(results, ctx, (r, w) -> w.array(r, Resp3Type.INTEGER));
   }
}
