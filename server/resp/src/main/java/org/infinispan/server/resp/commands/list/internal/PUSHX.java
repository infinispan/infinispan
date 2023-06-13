package org.infinispan.server.resp.commands.list.internal;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Abstract class for common code on PUSHX operations.
 *
 * @since 15.0
 */
public abstract class PUSHX extends PUSH implements Resp3Command {
   public PUSHX(boolean first) {
      super(first);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      EmbeddedMultimapListCache<byte[], byte[]> listCache = handler.getListMultimap();
      byte[] key = arguments.get(0);
      CompletionStage<Boolean> containsKey = listCache.containsKey(key);
      return CompletionStages.handleAndCompose(containsKey, (exists, t1) -> {
         if (exists) {
            return pushAndReturn(handler, ctx, arguments);
         }
         return handler
               .stageToReturn(CompletableFuture.completedFuture(0L),
                     ctx, Consumers.LONG_BICONSUMER);
      });
   }
}
