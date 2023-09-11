package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * PERSIST Resp Command
 * <a href="https://redis.io/commands/persist/">persist</a>
 *
 * @since 15.0
 */
public class PERSIST extends RespCommand implements Resp3Command {
   public PERSIST() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      return handler.stageToReturn(persist(handler, keyBytes), ctx, Consumers.LONG_BICONSUMER);
   }

   private static CompletableFuture<Long> persist(Resp3Handler handler, byte[] keyBytes) {
      return handler.cache().getCacheEntryAsync(keyBytes).thenCompose(e -> {
         if (e == null || e.getLifespan() < 0) {
            return EXPIRE.NOT_APPLIED;
         } else {
            return handler.cache().replaceAsync(e.getKey(), e.getValue(), e.getValue())
                  .thenCompose(replaced -> replaced ? EXPIRE.APPLIED : persist(handler, keyBytes));
         }
      });
   }
}
