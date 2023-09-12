package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
      var entries = new HashMap<byte[], byte[]>();
      for (int i = 0; i < arguments.size(); i++) {
         entries.put(arguments.get(i), arguments.get(++i));
      }
      // Change to loop?
      var existingEntries = handler.cache().getAll(entries.keySet());
      if (existingEntries.size() == 0) {
         return handler.stageToReturn(handler.cache().putAllAsync(entries).thenApply(v -> 1L), ctx,
               Consumers.LONG_BICONSUMER);
      }
      return handler
            .stageToReturn(CompletableFuture.completedFuture(0L),
                  ctx, Consumers.LONG_BICONSUMER);
   }
}
