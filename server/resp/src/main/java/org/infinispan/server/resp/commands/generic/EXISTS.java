package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * EXISTS Resp Command
 *
 * @link https://redis.io/commands/exists/
 * @since 15.0
 */
public class EXISTS extends RespCommand implements Resp3Command {
   public EXISTS() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      AtomicLong presentCount = new AtomicLong(arguments.size());
      for (byte[] bs : arguments) {
         acs.dependsOn(handler.cache().touch(bs, false).thenApply((v) -> {
            if (!v) {
               presentCount.decrementAndGet();
            }
            return null;
         }));
      }
      return handler.stageToReturn(acs.freeze().thenApply(v -> presentCount.get()), ctx,
            Consumers.LONG_BICONSUMER);
   }
}
