package org.infinispan.server.resp.commands.list.internal;

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
 * Abstract class for common code on PUSH operations
 *
 * @since 15.0
 */
public abstract class PUSH extends RespCommand implements Resp3Command {
   protected boolean first;

   public PUSH(boolean first) {
      super(-3, 1, 1, 1);
      this.first = first;
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

      return pushAndReturn(handler, ctx, arguments);
   }

   protected CompletionStage<RespRequestHandler> pushAndReturn(Resp3Handler handler,
                                                               ChannelHandlerContext ctx,
                                                               List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      // TODO: putAll operation on listMultimap ?
      for (int i = 1; i < arguments.size(); i++) {
         CompletionStage<Void> push = first ? listMultimap.offerFirst(key, arguments.get(i)) : listMultimap.offerLast(key, arguments.get(i));
         aggregateCompletionStage.dependsOn(push);
      }

      return CompletionStages.handleAndCompose(aggregateCompletionStage.freeze(), (r, t) -> {
         if (t != null) {
            if (t instanceof CacheException) {
               CacheException cacheException = (CacheException) t;
               if (cacheException.getCause() instanceof ClassCastException) {
                  RespErrorUtil.wrongType(handler.allocatorToUse());
                  return handler.myStage();
               }
            }
            throw new RuntimeException(t);
         }

         return handler.stageToReturn(listMultimap.size(key), ctx, Consumers.LONG_BICONSUMER);
      });
   }
}
