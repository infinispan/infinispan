package org.infinispan.server.resp.commands.generic;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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

      CompletableFuture<Long> totalTouchCount = CompletableFutures.sequence(
            arguments.stream()
                  .map(key -> (CompletableFuture<Boolean>) handler.cache().touch(key, true))
                  .collect(Collectors.toList())
      ).thenApply(result -> result.stream().filter(r -> r.booleanValue()).count());

      return handler.stageToReturn(totalTouchCount, ctx, Consumers.LONG_BICONSUMER);
   }
}
