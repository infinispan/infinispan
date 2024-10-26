package org.infinispan.server.resp.commands.generic;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * EXISTS
 *
 * @see <a href="https://redis.io/commands/exists/">EXISTS</a>
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
      AtomicLong presentCount = new AtomicLong(arguments.size());
      AggregateCompletionStage<AtomicLong> acs = CompletionStages.aggregateCompletionStage(presentCount);
      for (byte[] bs : arguments) {
         acs.dependsOn(handler.cache().touch(bs, false).thenApply((v) -> {
            if (!v) {
               presentCount.decrementAndGet();
            }
            return null;
         }));
      }
      return handler.stageToReturn(acs.freeze(), ctx, Resp3Response.INTEGER);
   }
}
