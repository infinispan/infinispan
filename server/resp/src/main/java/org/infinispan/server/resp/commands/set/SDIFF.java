package org.infinispan.server.resp.commands.set;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SDIFF
 *
 * @see <a href="https://redis.io/commands/sdiff/">SDIFF</a>
 * @since 15.0
 */
public class SDIFF extends RespCommand implements Resp3Command {

   public SDIFF() {
      super(-2, 1, -1, 1, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      // If the diff is with the set itself the result will be an empty set
      // but we need to go through the process to check all the keys are set type
      boolean diffItself = arguments.stream().skip(1)
            .anyMatch(item -> Objects.deepEquals(arguments.get(0), item));
      // Wrapping to exclude duplicate keys
      var uniqueKeys = SINTER.getUniqueKeys(handler, arguments);
      var allEntries = esc.getAll(uniqueKeys);
      return handler.stageToReturn(allEntries.thenApply(entriesMap -> diff(arguments.get(0), entriesMap, diffItself)),
            ctx, ResponseWriter.SET_BULK_STRING);
   }

   public static Set<byte[]> diff(byte[] key, Map<byte[], SetBucket<byte[]>> buckets, boolean diffItself) {
      Set<byte[]> minuend = Collections.emptySet();
      if (!diffItself) {
         byte[] kInMap = getKeyForMap(key, buckets);
         if (kInMap != null) {
            minuend = buckets.get(kInMap).toSet();
         }
         buckets.remove(kInMap);
      }
      for (var bucket : buckets.values()) {
         for (byte[] item : bucket.toList()) {
            if (minuend.isEmpty()) {
               break;
            }
            minuend.removeIf(v -> Objects.deepEquals(v, item));
         }
      }
      return minuend;
   }

   static byte[] getKeyForMap(byte[] key, Map<byte[], SetBucket<byte[]>> buckets) {
      if (buckets.isEmpty())
         return null;
      return buckets.keySet().stream().filter(item -> Arrays.equals(item, key)).findFirst().orElse(null);
   }
}
