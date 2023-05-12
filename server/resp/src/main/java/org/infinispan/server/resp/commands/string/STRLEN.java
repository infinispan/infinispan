package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * STRLEN Resp Command
 *
 * @link https://redis.io/commands/strlen/
 * @since 15.0
 */
public class STRLEN extends RespCommand implements Resp3Command {
   public STRLEN() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);

      CompletableFuture<Long> strLenAsync = handler.cache().getAsync(keyBytes)
      .thenApply(buff -> buff!=null ? buff.length : 0L);
      return handler.stageToReturn(strLenAsync, ctx, Consumers.LONG_BICONSUMER);
   }
}
