package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.SetOperation;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/set/
 * @since 14.0
 */
public class SET extends RespCommand implements Resp3Command {
   public SET() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() != 2) {
         return handler
               .stageToReturn(SetOperation.performOperation(handler.cache(), arguments, handler.respServer().getTimeService()), ctx,
                     Consumers.SET_BICONSUMER);
      }
      return handler.stageToReturn(
            handler.ignorePreviousValuesCache().putAsync(arguments.get(0), arguments.get(1)),
            ctx, Consumers.OK_BICONSUMER);
   }
}
