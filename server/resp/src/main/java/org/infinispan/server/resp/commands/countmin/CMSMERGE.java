package org.infinispan.server.resp.commands.countmin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
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
      // No @slow: matches COMMAND INFO output, despite docs claiming @slow
      super("CMS.MERGE", -4, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] destKey = arguments.get(0);
      int numKeys;
      try {
         numKeys = ArgumentUtils.toInt(arguments.get(1));
      } catch (NumberFormatException e) {
         handler.writer().customError(ProbabilisticErrors.CMS_INVALID_NUMKEYS);
         return handler.myStage();
      }

      if (numKeys <= 0) {
         handler.writer().customError(ProbabilisticErrors.CMS_NUMKEYS_POSITIVE);
         return handler.myStage();
      }

      if (arguments.size() < 2 + numKeys) {
         handler.writer().customError(ProbabilisticErrors.CMS_WRONG_NUM_KEYS);
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
                  weights.add(ArgumentUtils.toDouble(arguments.get(i)));
               } catch (NumberFormatException e) {
                  handler.writer().customError(ProbabilisticErrors.CMS_INVALID_WEIGHT);
                  return handler.myStage();
               }
            }
         }
      }

      // Fill in default weights if not enough provided
      while (weights.size() < numKeys) {
         weights.add(1.0);
      }

      AdvancedCache<byte[], Object> cache = handler.typedCache(null);

      CompletionStage<List<CountMinSketch>> sketches = CompletionStages.performSequentially(
            sourceKeys.iterator(),
            srcKey -> cache.getAsync(srcKey)
                  .thenApply(obj -> {
                     if (obj == null) {
                        throw new IllegalStateException(ProbabilisticErrors.CMS_KEY_NOT_FOUND);
                     }
                     return (CountMinSketch) obj;
                  }), Collectors.toList());

      CompletionStage<Boolean> result = sketches.thenCompose(sources -> {
         FunctionalMap.ReadWriteMap<byte[], Object> rwCache =
               FunctionalMap.create(cache).toReadWriteMap();

         CmsMergeFunction function = new CmsMergeFunction(sources, weights);
         return rwCache.eval(destKey, function);
      });

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
