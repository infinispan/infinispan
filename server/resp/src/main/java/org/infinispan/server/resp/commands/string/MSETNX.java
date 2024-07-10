package org.infinispan.server.resp.commands.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/msetnx/
 *       Derogating to the above description, this implementation is not atomic:
 *       msetnx first checks the nonexistence of all the keys
 *       and then performs the set. A concurrent set of any of the keys by
 *       another client
 *       will be overwritten.
 * @since 15.0
 */
public class MSETNX extends RespCommand implements Resp3Command {
   public MSETNX() {
      super(-3, 1, -1, 2);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      if (arguments.size() < 2 || arguments.size() % 2 != 0) {
         ByteBufferUtils.stringToByteBuf("-ERR wrong number of arguments for 'msetnx' command\r\n",
               handler.allocator());
         return handler.myStage();
      }
      Log.SERVER.msetnxConsistencyMessage();
      // Using WBA so equals() ensures same key has only one entry with the last value in the map
      var entriesWBA = new HashMap<WrappedByteArray, byte[]>();
      for (int i = 0; i < arguments.size(); i++) {
         entriesWBA.put(new WrappedByteArray(arguments.get(i)), arguments.get(++i));
      }
      // Change to loop?
      Map<byte[],byte[]> entries = entriesWBA.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getBytes(), e -> e.getValue()));
      var existingEntries = handler.cache().getAll(entriesWBA.keySet());
      if (existingEntries.isEmpty()) {
         return handler.stageToReturn(handler.cache().putAllAsync(entries).thenApply(v -> 1L), ctx,
               Consumers.LONG_BICONSUMER);
      }
      return handler
            .stageToReturn(CompletableFuture.completedFuture(0L),
                  ctx, Consumers.LONG_BICONSUMER);
   }
}
