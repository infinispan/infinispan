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
 * PTTL Resp Command
 * <a href="https://redis.io/commands/pttl/">pttl</a>
 * @since 15.0
 */
public class PTTL extends RespCommand implements Resp3Command {
   public PTTL() {
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
            return e.getLifespan();
         }
      }), ctx, Consumers.LONG_BICONSUMER);
   }
}
