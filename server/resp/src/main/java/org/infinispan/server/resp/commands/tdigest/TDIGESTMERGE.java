package org.infinispan.server.resp.commands.tdigest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TDIGEST.MERGE destKey numKeys src [src ...] [COMPRESSION compression]
 * <p>
 * Merges multiple t-digests into one.
 *
 * @see <a href="https://redis.io/commands/tdigest.merge/">TDIGEST.MERGE</a>
 * @since 16.2
 */
public class TDIGESTMERGE extends RespCommand implements Resp3Command {

   public TDIGESTMERGE() {
      super("TDIGEST.MERGE", -4, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] destKey = arguments.get(0);
      int numKeys;
      try {
         numKeys = Integer.parseInt(new String(arguments.get(1)));
      } catch (NumberFormatException e) {
         handler.writer().customError("ERR invalid numKeys");
         return handler.myStage();
      }

      if (numKeys <= 0) {
         handler.writer().customError("ERR numKeys must be positive");
         return handler.myStage();
      }

      if (arguments.size() < 2 + numKeys) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      List<byte[]> sourceKeys = new ArrayList<>();
      for (int i = 0; i < numKeys; i++) {
         sourceKeys.add(arguments.get(2 + i));
      }

      int compression = TDigest.DEFAULT_COMPRESSION;
      int idx = 2 + numKeys;
      if (idx < arguments.size()) {
         String option = new String(arguments.get(idx)).toUpperCase();
         if ("COMPRESSION".equals(option) && idx + 1 < arguments.size()) {
            try {
               compression = Integer.parseInt(new String(arguments.get(idx + 1)));
            } catch (NumberFormatException e) {
               handler.writer().customError("ERR invalid compression value");
               return handler.myStage();
            }
         }
      }

      AdvancedCache<byte[], Object> cache = handler.typedCache(null);

      List<CompletableFuture<TDigest>> futures = new ArrayList<>();
      for (byte[] srcKey : sourceKeys) {
         CompletableFuture<TDigest> future = cache.getAsync(srcKey)
               .thenApply(obj -> {
                  if (obj == null) {
                     throw new IllegalStateException("ERR one or more sources does not exist");
                  }
                  return (TDigest) obj;
               }).toCompletableFuture();
         futures.add(future);
      }

      int finalCompression = compression;
      CompletionStage<Boolean> result = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(v -> {
               List<TDigest> sources = new ArrayList<>();
               for (CompletableFuture<TDigest> f : futures) {
                  sources.add(f.join());
               }

               FunctionalMap.ReadWriteMap<byte[], Object> rwCache =
                     FunctionalMap.create(cache).toReadWriteMap();

               TDigestMergeFunction function = new TDigestMergeFunction(sources, finalCompression);
               return rwCache.eval(destKey, function);
            });

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
