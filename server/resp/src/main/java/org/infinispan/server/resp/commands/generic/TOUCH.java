package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * TOUCH
 *
 * @see <a href="https://redis.io/commands/touch/">TOUCH</a>
 * @since 15.0
 */
public class TOUCH extends RespCommand implements Resp3Command {
   public TOUCH() {
      super(-2, 1, -1, 1);
   }
   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      CompletionStage<Long> totalTouchCount = CompletionStages.performSequentially(arguments.iterator(),
            k -> handler.cache().touch(k, false),
            Collectors.summingLong(touched -> touched ? 1 : 0));

      return handler.stageToReturn(totalTouchCount, ctx, Resp3Response.INTEGER);
   }
}
