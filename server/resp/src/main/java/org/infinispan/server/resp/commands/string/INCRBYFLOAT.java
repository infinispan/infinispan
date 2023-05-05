package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/incrbyfloat/
 * @since 15.0
 */
public class INCRBYFLOAT extends RespCommand implements Resp3Command {
   public INCRBYFLOAT() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      return handler
            .stageToReturn(
                  CounterIncOrDec.counterIncByDouble(handler.cache(), arguments.get(0),
                        new String(arguments.get(1), StandardCharsets.US_ASCII)),
                  ctx, Consumers.DOUBLE_BICONSUMER);
   }
}
