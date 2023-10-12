package org.infinispan.server.resp.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Alters the last access time of a key(s). A key is ignored if it does not exist.
 * Integer reply: The number of keys that were touched.
 *
 * @link https://redis.io/commands/touch/
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

      return handler.stageToReturn(totalTouchCount, ctx, Consumers.LONG_BICONSUMER);
   }
}
