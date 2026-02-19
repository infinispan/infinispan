package org.infinispan.server.resp.commands.hll;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.hll.HyperLogLog;

import io.netty.channel.ChannelHandlerContext;

/**
 * PFMERGE
 *
 * @see <a href="https://redis.io/commands/pfmerge/">PFMERGE</a>
 * @since 16.2
 */
public class PFMERGE extends RespCommand implements Resp3Command {

   public PFMERGE() {
      super(-2, 1, -1, 1, AclCategory.WRITE.mask() | AclCategory.HYPERLOGLOG.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] destKey = arguments.get(0);
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);

      // Collect unique source keys (excluding destination).
      Set<byte[]> sourceKeys = arguments.subList(1, arguments.size()).stream()
            .map(WrappedByteArray::new)
            .collect(Collectors.toSet())
            .stream()
            .map(WrappedByteArray::getBytes)
            .collect(Collectors.toSet());

      CompletionStage<Map<byte[], Object>> cs = cache.getAllAsync(sourceKeys);
      return handler.stageToReturn(cs.thenCompose(entries -> merge(cache, destKey, entries)), ctx, (res, writer) -> {
         if (res) {
            writer.ok();
         } else {
            writer.wrongType();
         }
      });
   }

   private static CompletionStage<Boolean> merge(AdvancedCache<byte[], Object> cache, byte[] destKey, Map<byte[], Object> entries) {
      HyperLogLog merged = new HyperLogLog();
      for (Object value : entries.values()) {
         if (value == null) continue;

         if (!(value instanceof HyperLogLog hll)) {
            return CompletableFutures.completedFalse();
         }
         merged.mergeWith(hll);
      }

      // Use read-write eval to atomically include the destination's existing value in the merge.
      FunctionalMap.ReadWriteMap<byte[], Object> rwMap = FunctionalMap.create(cache).toReadWriteMap();
      return rwMap.eval(destKey, view -> {
         Object existing = view.find().orElse(null);
         if (existing != null && !(existing instanceof HyperLogLog)) {
            return false;
         }
         if (existing instanceof HyperLogLog existingHll) {
            merged.mergeWith(existingHll);
         }
         view.set(merged);
         return true;
      });
   }
}
