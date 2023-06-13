package org.infinispan.server.resp.commands.list.internal;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Abstract class for common code on POP operations
 *
 * @since 15.0
 */
public abstract class POP extends RespCommand implements Resp3Command {
   protected boolean first;

   public POP(boolean first) {
      super(-2, 1, 1, 1);
      this.first = first;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return popAndReturn(handler, ctx, arguments);
   }

   protected CompletionStage<RespRequestHandler> popAndReturn(Resp3Handler handler,
                                                               ChannelHandlerContext ctx,
                                                               List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      final long count;
      if (arguments.size() > 1) {
         count = ArgumentUtils.toLong(arguments.get(1));
         if (count < 0) {
            RespErrorUtil.mustBePositive(handler.allocator());
            return handler.myStage();
         }
      } else {
         count = 1;
      }

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Collection<byte[]>> pollValues = first ?
            listMultimap.pollFirst(key, count) :
            listMultimap.pollLast(key, count);
      return CompletionStages.handleAndCompose(pollValues ,(c, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         if (c == null) {
            return handler.stageToReturn(CompletableFuture.completedFuture(null), ctx, Consumers.GET_BICONSUMER);
         }

         if (c.size() == 1 && arguments.size() == 1) {
            // one single element and count argument was not present, return the value as string
            return handler.stageToReturn(CompletableFuture.completedFuture(c.iterator().next()), ctx, Consumers.GET_BICONSUMER);
         }

         // return an array of strings
         return handler.stageToReturn(CompletableFuture.completedFuture(c), ctx, Consumers.GET_ARRAY_BICONSUMER);
      });
   }
}
