package org.infinispan.server.resp.commands.sortedset;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.multimap.impl.SortedSetBucket.AggregateFunction.SUM;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZINTERCARD
 *
 * @see <a href="https://redis.io/commands/zintercard/">ZINTERCARD</a>
 * @since 15.0
 */
public class ZINTERCARD extends RespCommand implements Resp3Command {

   public static final String LIMIT = "LIMIT";

   public ZINTERCARD() {
      super(-3, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.SORTEDSET | AclCategory.SLOW;
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

      // unlimited
      int limit = -1;
      if (pos < arguments.size()) {
         String arg = new String(arguments.get(pos++));
         if (!LIMIT.equals(arg.toUpperCase())) {
            handler.writer().syntaxError();
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
         if (invalidLimit) {
            handler.writer().customError("LIMIT can't be negative");
            return handler.myStage();
         }
      }

      if (pos < arguments.size()) {
         // No more arguments are expected at this point
         handler.writer().syntaxError();
         return handler.myStage();
      }

      EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetCache = handler.getSortedSeMultimap();
      CompletionStage<Collection<ScoredValue<byte[]>>> aggValues = sortedSetCache
            .inter(keys.get(0), null, 1, SUM);
      final int finalLimit = limit;
      for (int i = 1; i < keys.size(); i++) {
         final byte[] setName = keys.get(i);
         aggValues = aggValues.thenCompose(c1 -> c1.isEmpty() || isLimitReached(c1.size(), finalLimit)
               ? completedFuture(c1)
               : sortedSetCache.inter(setName, c1, 1, SUM));
      }

      CompletionStage<Long> cs = aggValues.thenApply(res -> cardinalityResult(res.size(), finalLimit));
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }

   private static boolean isLimitReached(int interResultSize, int finalLimit) {
      // 0 or negative is unlimited
      return finalLimit > 0 && interResultSize >= finalLimit;
   }

   private static long cardinalityResult(int interResultSize, int finalLimit) {
      return finalLimit > 0 && interResultSize > finalLimit ? finalLimit : interResultSize;
   }
}
