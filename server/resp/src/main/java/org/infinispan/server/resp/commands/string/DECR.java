package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/decr/
 * @since 14.0
 */
public class DECR extends RespCommand implements Resp3Command {
   public DECR() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return handler.stageToReturn(CounterIncOrDec.counterIncOrDec(handler.cache(), arguments.get(0), false), ctx, Consumers.LONG_BICONSUMER);
   }
}
