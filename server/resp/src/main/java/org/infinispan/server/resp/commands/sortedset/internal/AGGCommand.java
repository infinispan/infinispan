package org.infinispan.server.resp.commands.sortedset.internal;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.mapResultsToArrayList;

/**
 * Common implementation for UNION and INTER commands
 */
public abstract class AGGCommand extends RespCommand implements Resp3Command {
   public static final String WEIGHTS = "WEIGHTS";
   public static final String AGGREGATE = "AGGREGATE";
   public static final String WITHSCORES = "WITHSCORES";
   private final AGGCommandType aggCommandType;
   protected enum AGGCommandType {
      UNION, INTER
   }
   protected AGGCommand(int arity, int firstKeyPos, int lastKeyPos, int steps, AGGCommandType aggCommandType) {
      super(arity, firstKeyPos, lastKeyPos, steps);
      this.aggCommandType = aggCommandType;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int pos = 0;
      final byte[] destination;
      if (getArity() == -4) {
         destination = arguments.get(pos++);
      } else {
         destination = null;
      }

      int numberOfKeysArg;
      try {
         numberOfKeysArg = ArgumentUtils.toInt(arguments.get(pos++));
      } catch (NumberFormatException ex) {
         RespErrorUtil.valueNotInteger(handler.allocator());
         return handler.myStage();
      }

      if (numberOfKeysArg <= 0) {
         RespErrorUtil.customError("at least 1 input key is needed for '" + this.getName().toLowerCase() + "' command", handler.allocator());
         return handler.myStage();
      }

      List<byte[]> keys = new ArrayList<>(numberOfKeysArg);
      for (int i = 0; (i < numberOfKeysArg && pos < arguments.size()); i++) {
         keys.add(arguments.get(pos++));
      }

      if (keys.size() < numberOfKeysArg) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      final List<Double> weights = new ArrayList<>();
      boolean withScores = false;
      SortedSetBucket.AggregateFunction aggOption = SortedSetBucket.AggregateFunction.SUM;
      while (pos < arguments.size()) {
         String arg = new String(arguments.get(pos++));
         switch (arg) {
            case WITHSCORES:
               if (getArity() == -3) {
                  withScores = true;
               } else {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               break;
            case AGGREGATE:
               if (pos < arguments.size()) {
                  try {
                     aggOption = SortedSetBucket.AggregateFunction.valueOf(new String(arguments.get(pos++)));
                  } catch (Exception ex) {
                     RespErrorUtil.syntaxError(handler.allocator());
                     return handler.myStage();
                  }
               } else {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               break;
            case WEIGHTS:
               try {
                  for (int i = 0; (i < numberOfKeysArg && pos < arguments.size()); i++) {
                     weights.add(ArgumentUtils.toDouble(arguments.get(pos++)));
                  }
               } catch (NumberFormatException ex) {
                  RespErrorUtil.customError("weight value is not a float", handler.allocator());
                  return handler.myStage();
               }
               if (weights.size() != numberOfKeysArg) {
                  RespErrorUtil.syntaxError(handler.allocator());
                  return handler.myStage();
               }
               break;
            default:
               RespErrorUtil.syntaxError(handler.allocator());
               return handler.myStage();

         }
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      final SortedSetBucket.AggregateFunction finalAggFunction = aggOption;
      CompletionStage<Collection<SortedSetBucket.ScoredValue<byte[]>>> aggValues;
      if (aggCommandType == AGGCommandType.UNION) {
         aggValues = sortedSetCache.union(keys.get(0), null, computeWeight(weights, 0), finalAggFunction);
      } else {
         aggValues = sortedSetCache.inter(keys.get(0), null, computeWeight(weights, 0), finalAggFunction);
      }

      for (int i = 1; i < keys.size(); i++) {
         final byte[] setName = keys.get(i);
         final double weight = computeWeight(weights, i);
         aggValues = aggValues.thenCompose(c1 -> {
            if (aggCommandType == AGGCommandType.UNION) {
               return sortedSetCache.union(setName, c1, weight, finalAggFunction);
            }

            return c1.isEmpty()
                  ? CompletableFuture.completedFuture(c1)
                  : sortedSetCache.inter(setName, c1, weight, finalAggFunction);
         });
      }
      final boolean finalWithScores = withScores;
      return CompletionStages.handleAndCompose(aggValues, (result, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         if (destination != null) {
            return handler.stageToReturn(sortedSetCache.addMany(destination, result, SortedSetAddArgs.create().replace().build()), ctx, Consumers.LONG_BICONSUMER);
         }

         return handler.stageToReturn(CompletableFuture.completedFuture(mapResultsToArrayList(result, finalWithScores)), ctx, Consumers.GET_ARRAY_BICONSUMER);
      });
   }

   private static double computeWeight(List<Double> weights, int index) {
      return weights.isEmpty() ? 1 : weights.get(index);
   }

}
