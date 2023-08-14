package org.infinispan.server.resp.commands.sortedset;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
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
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.multimap.impl.SortedSetBucket.AggregateFunction.SUM;

/**
 * This command is similar to {@link ZINTER}, but instead of returning the resulting sorted set,
 * it returns just the cardinality of the result.
 *
 * Keys that do not exist are considered to be empty sets. With one of the keys being an empty set,
 * the resulting set is also empty (since set intersection with an empty set always results in an empty set).
 *
 * By default, the command calculates the cardinality of the intersection of all given sets.
 * When provided with the optional LIMIT argument (which defaults to 0 and means unlimited),
 * if the intersection cardinality reaches limit partway through the computation,
 * the algorithm will exit and yield limit as the cardinality. Such implementation ensures a
 * significant speedup for queries where the limit is lower than the actual intersection cardinality.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zintercard/">Redis Documentation</a>
 */
public class ZINTERCARD extends RespCommand implements Resp3Command {

   public static final String LIMIT = "LIMIT";

   public ZINTERCARD() {
      super(-3, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int pos = 0;
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

      // unlimited
      int limit = -1;
      if (pos < arguments.size()) {
         String arg = new String(arguments.get(pos++));
         if (!LIMIT.equals(arg.toUpperCase())) {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
         boolean invalidLimit = false;
         try {
            limit = ArgumentUtils.toInt(arguments.get(pos++));
            if (limit < 0) {
               invalidLimit = true;
            }
         } catch (NumberFormatException ex) {
            invalidLimit = true;
         }
         if (invalidLimit){
            RespErrorUtil.customError("LIMIT can't be negative", handler.allocator());
            return handler.myStage();
         }
      }

      if (pos < arguments.size()) {
         // No more arguments are expected at this point
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      CompletionStage<Collection<SortedSetBucket.ScoredValue<byte[]>>> aggValues = sortedSetCache
            .inter(keys.get(0), null, 1,  SUM);
      final int finalLimit = limit;
      for (int i = 1; i < keys.size(); i++) {
         final byte[] setName = keys.get(i);
         aggValues = aggValues.thenCompose(c1 -> c1.isEmpty() || isLimitReached(c1.size(), finalLimit)
               ? completedFuture(c1)
               : sortedSetCache.inter(setName, c1, 1, SUM));
      }

      return CompletionStages.handleAndCompose(aggValues, (interResult, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         return handler.stageToReturn(completedFuture(cardinalityResult(interResult.size(), finalLimit)),
               ctx, Consumers.LONG_BICONSUMER);
      });
   }

   private static boolean isLimitReached(int interResultSize, int finalLimit) {
      // 0 or negative is unlimited
      return finalLimit > 0 && interResultSize >= finalLimit;
   }

   private static long cardinalityResult(int interResultSize, int finalLimit) {
      return finalLimit > 0 && interResultSize > finalLimit ? finalLimit : interResultSize;
   }
}
