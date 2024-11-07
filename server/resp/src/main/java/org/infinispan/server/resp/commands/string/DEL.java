package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * DEL
 *
 * @see <a href="https://redis.io/commands/del/">DEL</a>
 * @since 14.0
 */
public class DEL extends RespCommand implements Resp3Command {

   public DEL() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      AtomicInteger removes = new AtomicInteger();
      AggregateCompletionStage<AtomicInteger> deleteStages = CompletionStages.aggregateCompletionStage(removes);
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      final AdvancedCache<byte[], Object> acm = handler.cache()
            .<byte[], Object>withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt);
      for (byte[] keyBytesLoop : arguments) {
         deleteStages.dependsOn(acm.removeAsync(keyBytesLoop)
               .thenAccept(prev -> {
                  if (prev != null) {
                     removes.incrementAndGet();
                  }
               }));
      }
      return handler.stageToReturn(deleteStages.freeze(), ctx, ResponseWriter.INTEGER);
   }
}
