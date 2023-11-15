package org.infinispan.server.resp.commands.list.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

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
      CompletionStage<Long> containsKey = listCache.containsKey(key)
            .thenCompose(exists -> exists
                  ? pushAndReturn(handler, arguments)
                  : CompletableFuture.completedFuture(0L));
      return handler.stageToReturn(containsKey, ctx, Consumers.LONG_BICONSUMER);
   }
}
