package org.infinispan.server.resp.commands.set;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SDIFFCARD numkeys key [key ...] [LIMIT limit]
 * <p>
 * Returns the cardinality of the difference between the first set and all the successive sets. With {@code LIMIT},
 * counting stops once the limit is reached ({@code 0} means no limit).
 *
 * @see <a href="https://redis.io/commands/sdiffcard/">SDIFFCARD</a>
 * @since 16.4
 */
public class SDIFFCARD extends RespCommand implements Resp3Command {
   private static final String LIMIT = "LIMIT";

   public SDIFFCARD() {
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
      if (arguments.size() < keysNum + 1) {
         handler.writer().customError("Number of keys can't be greater than number of args");
         return handler.myStage();
      }

      int limit = 0;
      for (int i = keysNum + 1; i < arguments.size(); i++) {
         String opt = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         if (LIMIT.equals(opt) && i + 1 < arguments.size()) {
            try {
               limit = ArgumentUtils.toInt(arguments.get(++i));
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
      List<byte[]> keyArgs = arguments.subList(1, keysNum + 1);
      byte[] first = keyArgs.get(0);
      // A set minus itself is empty, but every key must still be type checked
      boolean diffItself = keyArgs.stream().skip(1).anyMatch(k -> Arrays.equals(first, k));
      Set<byte[]> keys = SINTER.getUniqueKeys(handler, keyArgs);
      final int finalLimit = limit;
      return handler.stageToReturn(
            esc.getAll(keys).thenApply(sets -> count(first, sets, diffItself, finalLimit)),
            ctx,
            ResponseWriter.INTEGER);
   }

   static long count(byte[] first, Map<byte[], SetBucket<byte[]>> sets, boolean diffItself, int limit) {
      // Wrong-type values only fail (ClassCastException -> WRONGTYPE) when iterated as SetBucket,
      // so read every bucket before any shortcut or LIMIT can end the count early
      Set<byte[]> minuend = null;
      Set<WrappedByteArray> subtrahend = new HashSet<>();
      for (Map.Entry<byte[], SetBucket<byte[]>> entry : sets.entrySet()) {
         SetBucket<byte[]> bucket = entry.getValue();
         if (bucket == null) continue;
         Set<byte[]> elements = bucket.toSet();
         if (Arrays.equals(first, entry.getKey())) {
            minuend = elements;
         } else {
            for (byte[] element : elements) {
               subtrahend.add(new WrappedByteArray(element));
            }
         }
      }
      if (minuend == null || diffItself) {
         return 0;
      }

      long count = 0;
      for (byte[] element : minuend) {
         if (!subtrahend.contains(new WrappedByteArray(element)) && ++count == limit) {
            break;
         }
      }
      return count;
   }
}
