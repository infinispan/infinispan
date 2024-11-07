package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * INCRBYFLOAT
 *
 * @see <a href="https://redis.io/commands/incrbyfloat/">INCRBYFLOAT</a>
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
                        ArgumentUtils.toDouble(arguments.get(1))),
                  ctx, ResponseWriter.DOUBLE);
   }
}
