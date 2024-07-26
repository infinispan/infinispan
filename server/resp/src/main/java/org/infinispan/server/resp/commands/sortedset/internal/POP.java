package org.infinispan.server.resp.commands.sortedset.internal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.response.ScoredValueSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * Common implementation for ZPOP commands
 */
public abstract class POP extends RespCommand implements Resp3Command {
   private static final BiConsumer<Object, ByteBufPool> SERIALIZER = (res, alloc) -> {
      if (res instanceof Collection<?>) {
         @SuppressWarnings("unchecked")
         Collection<ScoredValue<byte[]>> cast = (Collection<ScoredValue<byte[]>>) res;
         Resp3Response.array(cast, alloc, ScoredValueSerializer.INSTANCE);
         return;
      }

      Resp3Response.write((ScoredValue<byte[]>) res, alloc, ScoredValueSerializer.INSTANCE);
   };

   private final boolean min;
   public POP(boolean min) {
      super(-2, 1, 1, 1);
      this.min = min;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] name = arguments.get(0);
      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();

      long count = 1;
      if (arguments.size() > 1) {
         try {
            count = ArgumentUtils.toLong(arguments.get(1));
            if (count < 0) {
               RespErrorUtil.mustBePositive(handler.allocator());
               return handler.myStage();
            }
         } catch (NumberFormatException e) {
            RespErrorUtil.mustBePositive(handler.allocator());
            return handler.myStage();
         }
      }

      long finalCount = count;
      CompletionStage<Object> popElements = sortedSetCache.pop(name, min, count).thenApply(r -> {
         if (r.isEmpty() || finalCount > 1) return r;
         return r.iterator().next();
      });
      return handler.stageToReturn(popElements, ctx, SERIALIZER);
   }
}
