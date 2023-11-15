package org.infinispan.server.resp.commands.generic;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * RENAME Resp Command
 *
 * @link <a href="https://redis.io/commands/rename/">RENAME</a>
 *       This operation is NOT atomic, it is performed in two step: old key
 *       removed, new key set.
 *       Data could get lost in case of failure of the latter.
 * @since 15.0
 */
public class RENAME extends RespCommand implements Resp3Command {

   public RENAME() {
      super(3, 1, 2, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] srcKey = arguments.get(0);
      byte[] dstKey = arguments.get(1);
      return rename(handler, srcKey, dstKey, ctx, Consumers.OK_BICONSUMER);
   }

   public static CompletionStage<RespRequestHandler> rename(Resp3Handler handler, byte[] srcKey, byte[] dstKey,
         ChannelHandlerContext ctx, BiConsumer<?, ByteBufPool> consumer) {
      BiConsumer<Object, ByteBufPool> bc = (BiConsumer<Object, ByteBufPool>) consumer;
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      final AdvancedCache<byte[], Object> acm = handler.typedCache(vmt);
      CompletionStage<?> cs = acm.removeAsyncEntry(srcKey)
            .thenCompose(e -> {
               if (e == null) return CompletableFutures.completedNull();

               if (Arrays.equals(srcKey, dstKey)) {
                  // If src = dest ...
                  return CompletableFuture.completedFuture(1L);
               } else {
                  var timeService = handler.respServer().getTimeService();
                  // ... else if Immortal entry case: copy metadata...
                  if (e.getLifespan() <= 0) {
                     var newMeta = e.getMetadata().builder();
                     return acm.putAsyncEntry(dstKey, e.getValue(), newMeta.build()).thenApply(ignore -> 1L);
                  } else {
                     long newLifespan = e.getLifespan() + e.getCreated() - timeService.wallClockTime();
                     if (newLifespan > 0) {
                        // ... else if not expired copy metadata and preserve lifespan...
                        var newMeta = e.getMetadata().builder().lifespan(newLifespan);
                        return acm.putAsyncEntry(dstKey, e.getValue(), newMeta.build()).thenApply(ignore -> 1L);
                     } else {
                        // ... or do nothing if expired
                        return CompletableFuture.completedFuture(1L);
                     }
                  }
               }
            });

      return handler.stageToReturn(cs, ctx, (l, bbp) -> {
         if (l != null) bc.accept(l, bbp);
         else RespErrorUtil.noSuchKey(handler.allocator());
      });
   }
}
