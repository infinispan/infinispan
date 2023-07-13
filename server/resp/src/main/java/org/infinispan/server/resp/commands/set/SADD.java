package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * Abstract class for common code on SADD operations
 *
 * @since 15.0
 */
public class SADD extends RespCommand implements Resp3Command {
   public SADD() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      EmbeddedSetCache<byte[],byte[]> esc = handler.getEmbeddedSetCache();
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      var addedCount = new AtomicLong(0);
      for (int i = 1; i < arguments.size(); i++) {
         CompletionStage<Void> add = esc.add(key, arguments.get(i)).thenAccept((v) -> {if (v) addedCount.incrementAndGet();});
         aggregateCompletionStage.dependsOn(add);
      }

      CompletionStage<Long> cs = aggregateCompletionStage.freeze().thenApply(ignore -> addedCount.get());
      return handler.stageToReturn(cs, ctx, Consumers.LONG_BICONSUMER);
   }
}
