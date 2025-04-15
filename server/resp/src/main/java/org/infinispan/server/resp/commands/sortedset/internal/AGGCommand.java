package org.infinispan.server.resp.commands.sortedset.internal;

import static org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetAddArgs;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.sortedset.ZSetCommonUtils;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * Common implementation for UNION and INTER commands
 */
public abstract class AGGCommand extends RespCommand implements Resp3Command {
   private static final BiConsumer<Object, ResponseWriter> SERIALIZER = (res, writer) -> {
      if (res instanceof Long l) {
         writer.integers(l);
         return;
      }

      ZSetCommonUtils.ZOperationResponse zres = (ZSetCommonUtils.ZOperationResponse) res;
      writer.write(zres, zres);
   };

   public static final String WEIGHTS = "WEIGHTS";
   public static final String AGGREGATE = "AGGREGATE";
   public static final String WITHSCORES = "WITHSCORES";
   private final AGGCommandType aggCommandType;
   protected enum AGGCommandType {
      UNION, INTER
   }
   protected AGGCommand(int arity, int firstKeyPos, int lastKeyPos, int steps, AGGCommandType aggCommandType, long aclMask) {
      super(arity, firstKeyPos, lastKeyPos, steps, aclMask);
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
         handler.writer().valueNotInteger();
         return handler.myStage();
      }

      if (numberOfKeysArg <= 0) {
         handler.writer().customError("at least 1 input key is needed for '" + this.getName().toLowerCase() + "' command");
         return handler.myStage();
      }

      List<byte[]> keys = new ArrayList<>(numberOfKeysArg);
      for (int i = 0; (i < numberOfKeysArg && pos < arguments.size()); i++) {
         keys.add(arguments.get(pos++));
      }

      if (keys.size() < numberOfKeysArg) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      final List<Double> weights = new ArrayList<>();
      boolean withScores = false;
      SortedSetBucket.AggregateFunction aggOption = SortedSetBucket.AggregateFunction.SUM;
      while (pos < arguments.size()) {
         String arg = new String(arguments.get(pos++)).toUpperCase();
         switch (arg) {
            case WITHSCORES:
               if (getArity() == -3) {
                  withScores = true;
               } else {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               break;
            case AGGREGATE:
               if (pos < arguments.size()) {
                  try {
                     aggOption = SortedSetBucket.AggregateFunction.valueOf(new String(arguments.get(pos++)).toUpperCase());
                  } catch (Exception ex) {
                     handler.writer().syntaxError();
                     return handler.myStage();
                  }
               } else {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               break;
            case WEIGHTS:
               try {
                  for (int i = 0; (i < numberOfKeysArg && pos < arguments.size()); i++) {
                     weights.add(ArgumentUtils.toDouble(arguments.get(pos++)));
                  }
               } catch (NumberFormatException ex) {
                  handler.writer().customError("weight value is not a float");
                  return handler.myStage();
               }
               if (weights.size() != numberOfKeysArg) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();

         }
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      final SortedSetBucket.AggregateFunction finalAggFunction = aggOption;
      CompletionStage<Collection<ScoredValue<byte[]>>> aggValues;
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
      CompletionStage<?> cs = aggValues
            .thenCompose(result -> {
               CompletionStage<?> n = destination != null
                     ? sortedSetCache.addMany(destination, result, SortedSetAddArgs.create().replace().build())
                     : CompletableFuture.completedFuture(response(result, finalWithScores));
               return n;
            });

      return handler.stageToReturn(cs, ctx, SERIALIZER);
   }

   private static double computeWeight(List<Double> weights, int index) {
      return weights.isEmpty() ? 1 : weights.get(index);
   }

}
