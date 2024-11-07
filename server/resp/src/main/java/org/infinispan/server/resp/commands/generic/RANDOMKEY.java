package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.AdvancedCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * RANDOMKEY
 *
 * @see <a href="https://redis.io/commands/randomkey/">RANDOMKEY</a>
 * @since 15.0
 */
public class RANDOMKEY extends RespCommand implements Resp3Command {

   public RANDOMKEY() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      AdvancedCache<byte[], byte[]> cache = handler.cache();
      CompletableFuture<byte[]> cs = cache.sizeAsync()
            .thenApply(size -> {
               if (size == 0)
                  return null;

               // Try to insert some randomness in the returned key.
               return cache.keySet().stream()
                     .skip(ThreadLocalRandom.current().nextInt(size.intValue()))
                     .findAny()
                     .orElse(null);
            });
      return handler.stageToReturn(cs, ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
