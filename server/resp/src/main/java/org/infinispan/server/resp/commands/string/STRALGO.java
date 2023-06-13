package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.LCSOperation;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/lcs/
 * @since 15.0
 */
public class STRALGO extends RespCommand implements Resp3Command {
   public STRALGO() {
      super(-5, 1, 2, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      return handler.stageToReturn(LCSOperation.performOperation(handler.cache(), arguments), ctx, Consumers.LCS_BICONSUMER);
   }


}
