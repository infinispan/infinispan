package org.infinispan.server.resp.commands.set;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.logging.Log;

import io.netty.channel.ChannelHandlerContext;

/**
 * SMOVE implementation
 * <p>
 * Atomicity warning:
 * Derogating to the above description, this implementation is not atomic:
 * is it possible that, moving an existing element in source, at a given
 * time a client
 * can observe that element doesn't exists both in source and destination.
 * @since 15.0
 * @see <a href="https://redis.io/commands/smove/">Redis documentation</a>
 */
public class SMOVE extends RespCommand implements Resp3Command {
   public SMOVE() {
      super(4, 1, 2, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      final var source = arguments.get(0);
      final var destination = arguments.get(1);
      final var element = arguments.get(2);

      boolean sameList = Arrays.equals(source, destination);
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      if (!sameList) {
         // warn when different sets
         Log.SERVER.smoveConsistencyMessage();
         return handler.stageToReturn(moveElement(esc, element, source, destination), ctx, Consumers.LONG_BICONSUMER);
      }
      return handler.stageToReturn(esc.get(source)
            .thenApply((bucket) -> bucket.contains(element) ? 1L : 0L), ctx, Consumers.LONG_BICONSUMER);
   }

   private CompletionStage<Long> moveElement(EmbeddedSetCache<byte[], byte[]> cache, byte[] element, byte[] srcKey, byte[] destKey) {
      // Check whether the destination set is a set structure.
      // Under concurrent load, this might malfunction as the entry is created concurrently elsewhere.
      return cache.exists(destKey)
            .thenCompose(ignore -> removeAndAdd(cache, element, srcKey, destKey));
   }

   private CompletionStage<Long> removeAndAdd(EmbeddedSetCache<byte[], byte[]> cache, byte[] element, byte[] srcKey, byte[] destKey) {
      return cache.remove(srcKey, element)
            .thenCompose(removed -> {
               if (removed == 0) return CompletableFuture.completedFuture(0L);

               return cache.add(destKey, element);
            });
   }
}
