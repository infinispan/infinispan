package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/rpushx/
 * Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list.
 * In contrary to {@link RPUSH} no operation will be performed when key does not yet exist.
 * Integer reply: the length of the list after the push operation.
 * @since 15.0
 */
public class RPUSHX extends RPUSH implements Resp3Command {
   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      if (arguments.size() < 2) {
         // ERROR
         RespErrorUtil.wrongArgumentNumber(this, handler.allocatorToUse());
         return handler.myStage();
      }

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
