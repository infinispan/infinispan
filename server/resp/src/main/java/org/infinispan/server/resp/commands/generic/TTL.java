package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TTL Resp Command
 * <a href="https://redis.io/commands/ttl/">ttl</a>
 * @since 15.0
 */
public class TTL extends RespCommand implements Resp3Command {
   public TTL() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      return handler.stageToReturn(handler.cache().getCacheEntryAsync(keyBytes).thenApply(e -> {
         if (e == null) {
            return -2L;
         } else {
            long ttl = e.getLifespan();
            return ttl < 0 ? ttl : ttl / 1000;
         }
      }), ctx, Consumers.LONG_BICONSUMER);
   }
}
