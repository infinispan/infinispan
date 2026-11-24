package org.infinispan.server.resp.commands.set;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.hll.HyperLogLog;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SUNIONCARD numkeys key [key ...] [APPROX] [LIMIT limit]
 * <p>
 * Returns the cardinality of the union of the given sets. With {@code LIMIT}, counting stops once the limit is
 * reached ({@code 0} means no limit). With {@code APPROX}, the cardinality is estimated with a HyperLogLog.
 *
 * @see <a href="https://redis.io/commands/sunioncard/">SUNIONCARD</a>
 * @since 16.4
 */
public class SUNIONCARD extends RespCommand implements Resp3Command {
   private static final byte[] APPROX = "APPROX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] LIMIT = "LIMIT".getBytes(StandardCharsets.US_ASCII);
   // In APPROX mode, how many HyperLogLog register updates between checks of the estimate against LIMIT
   private static final int APPROX_LIMIT_CHECK_INTERVAL = 1024;

   public SUNIONCARD() {
      super(-3, 0, 0, 0, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int keysNum = 0;
      try {
         keysNum = ArgumentUtils.toInt(arguments.get(0));
      } catch (NumberFormatException ignore) {
      }
      if (keysNum <= 0) {
         handler.writer().customError("numkeys should be greater than 0");
         return handler.myStage();
      }
      if (keysNum > arguments.size() - 1) {
         handler.writer().customError("Number of keys can't be greater than number of args");
         return handler.myStage();
      }

      boolean approx = false;
      long limit = 0;
      for (int i = keysNum + 1; i < arguments.size(); i++) {
         byte[] opt = arguments.get(i);
         if (RespUtil.isAsciiBytesEquals(APPROX, opt)) {
            approx = true;
         } else if (RespUtil.isAsciiBytesEquals(LIMIT, opt) && i + 1 < arguments.size()) {
            try {
               limit = ArgumentUtils.toLong(arguments.get(++i));
            } catch (NumberFormatException e) {
               limit = -1;
            }
            if (limit < 0) {
               // Same message as Redis, also for a non-integer limit
               handler.writer().customError("LIMIT can't be negative");
               return handler.myStage();
            }
         } else {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      }

      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      Set<byte[]> keys = SINTER.getUniqueKeys(handler, arguments.subList(1, keysNum + 1));
      final long finalLimit = limit;
      final boolean finalApprox = approx;
      return handler.stageToReturn(
            esc.getAll(keys).thenApply(sets -> count(sets.values(), finalLimit, finalApprox)),
            ctx,
            ResponseWriter.INTEGER);
   }

   static long count(Collection<SetBucket<byte[]>> sets, long limit, boolean approx) {
      // Wrong-type values only fail (ClassCastException -> WRONGTYPE) when iterated as SetBucket,
      // so check every bucket before LIMIT can stop the count early
      List<SetBucket<byte[]>> buckets = new ArrayList<>(sets.size());
      for (SetBucket<byte[]> bucket : sets) {
         if (bucket != null) buckets.add(bucket);
      }
      long count = approx ? approxCount(buckets, limit) : exactCount(buckets, limit);
      return limit > 0 ? Math.min(count, limit) : count;
   }

   private static long exactCount(List<SetBucket<byte[]>> buckets, long limit) {
      Set<MultimapObjectWrapper<byte[]>> seen = new HashSet<>();
      for (SetBucket<byte[]> bucket : buckets) {
         for (Iterator<MultimapObjectWrapper<byte[]>> it = bucket.stream().iterator(); it.hasNext(); ) {
            if (seen.add(it.next()) && seen.size() == limit) {
               return limit;
            }
         }
      }
      return seen.size();
   }

   private static long approxCount(List<SetBucket<byte[]>> buckets, long limit) {
      HyperLogLog hll = new HyperLogLog();
      int updates = 0;
      for (SetBucket<byte[]> bucket : buckets) {
         for (Iterator<MultimapObjectWrapper<byte[]>> it = bucket.stream().iterator(); it.hasNext(); ) {
            // The estimate only changes when a register changes; computing it is not free, so check periodically
            if (hll.add(it.next().get()) && limit > 0 && ++updates % APPROX_LIMIT_CHECK_INTERVAL == 0
                  && hll.cardinality() >= limit) {
               return limit;
            }
         }
      }
      return hll.cardinality();
   }
}
