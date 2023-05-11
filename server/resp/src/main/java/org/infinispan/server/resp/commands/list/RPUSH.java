package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.CacheException;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/rpush/
 *
 * Integer reply: the length of the list after the push operation.
 * @since 15.0
 */
public class RPUSH extends RespCommand implements Resp3Command {
   public RPUSH() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      if (arguments.size() < 2) {
         // ERROR
         RespErrorUtil.wrongArgumentNumber(this, handler.allocatorToUse());
         return handler.myStage();
      }

      // TODO: putAll operation on multimap ?
      return pushAndReturn(handler, ctx, arguments);
   }

   protected CompletionStage<RespRequestHandler> pushAndReturn(Resp3Handler handler,
                                                               ChannelHandlerContext ctx,
                                                               List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      for (int i = 1; i < arguments.size(); i++) {
         aggregateCompletionStage.dependsOn(listMultimap.offerLast(key, arguments.get(i)));
      }

      return CompletionStages.handleAndCompose(aggregateCompletionStage.freeze(), (r, t) -> {
         if (t instanceof CacheException) {
            CacheException cacheException = (CacheException) t;
            if (cacheException.getCause() instanceof ClassCastException) {
               RespErrorUtil.wrongType(handler.allocatorToUse());
               return handler.myStage();
            }
            throw cacheException;
         }

         return handler.stageToReturn(listMultimap.size(key), ctx, Consumers.LONG_BICONSUMER);
      });
   }
}
