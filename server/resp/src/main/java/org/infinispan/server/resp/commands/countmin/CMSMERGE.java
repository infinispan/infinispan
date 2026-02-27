package org.infinispan.server.resp.commands.countmin;

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
 * CMS.MERGE destKey numKeys src [src ...] [WEIGHTS weight [weight ...]]
 * <p>
 * Merges several sketches into one sketch.
 *
 * @see <a href="https://redis.io/commands/cms.merge/">CMS.MERGE</a>
 * @since 16.2
 */
public class CMSMERGE extends RespCommand implements Resp3Command {

   public CMSMERGE() {
      super("CMS.MERGE", -4, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.WRITE.mask());
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

      List<Double> weights = new ArrayList<>();
      int weightsIdx = 2 + numKeys;
      if (weightsIdx < arguments.size()) {
         String option = new String(arguments.get(weightsIdx)).toUpperCase();
         if ("WEIGHTS".equals(option)) {
            for (int i = weightsIdx + 1; i < arguments.size(); i++) {
               try {
                  weights.add(Double.parseDouble(new String(arguments.get(i))));
               } catch (NumberFormatException e) {
                  handler.writer().customError("ERR invalid weight value");
                  return handler.myStage();
               }
            }
         }
      }

      // Fill in default weights if not enough provided
      while (weights.size() < numKeys) {
         weights.add(1.0);
      }

      // First, we need to read all source sketches
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);

      List<CompletableFuture<CountMinSketch>> futures = new ArrayList<>();
      for (byte[] srcKey : sourceKeys) {
         CompletableFuture<CountMinSketch> future = cache.getAsync(srcKey)
               .thenApply(obj -> {
                  if (obj == null) {
                     throw new IllegalStateException("ERR one or more sources does not exist");
                  }
                  return (CountMinSketch) obj;
               }).toCompletableFuture();
         futures.add(future);
      }

      CompletionStage<Boolean> result = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(v -> {
               List<CountMinSketch> sources = new ArrayList<>();
               for (CompletableFuture<CountMinSketch> f : futures) {
                  sources.add(f.join());
               }

               FunctionalMap.ReadWriteMap<byte[], Object> rwCache =
                     FunctionalMap.create(cache).toReadWriteMap();

               CmsMergeFunction function = new CmsMergeFunction(sources, weights);
               return rwCache.eval(destKey, function);
            });

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
