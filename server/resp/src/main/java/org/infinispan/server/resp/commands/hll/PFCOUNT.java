package org.infinispan.server.resp.commands.hll;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.hll.HyperLogLog;

import io.netty.channel.ChannelHandlerContext;

/**
 * PFCOUNT
 *
 * @see <a href="https://redis.io/commands/pfcount/">PFCOUNT</a>
 * @since 16.2
 */
public class PFCOUNT extends RespCommand implements Resp3Command {

   public PFCOUNT() {
      super(-2, 1, -1, 1, AclCategory.READ.mask() | AclCategory.HYPERLOGLOG.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);

      Set<byte[]> uniqueKeys = arguments.stream()
            .map(WrappedByteArray::new)
            .collect(Collectors.toSet())
            .stream()
            .map(WrappedByteArray::getBytes)
            .collect(Collectors.toSet());

      CompletionStage<Map<byte[], Object>> cs = cache.getAllAsync(uniqueKeys);
      return handler.stageToReturn(cs.thenApply(entries -> cardinality(entries, uniqueKeys.size())), ctx, (res, writer) -> {
         if (res >= 0) {
            writer.integers(res);
         } else {
            writer.wrongType();
         }
      });
   }

   private static long cardinality(Map<byte[], Object> entries, int requestedKeys) {
      HyperLogLog merged = null;
      boolean singleKey = requestedKeys == 1;

      for (Object value : entries.values()) {
         if (value == null) continue;

         if (!(value instanceof HyperLogLog hll)) {
            return -1;
         }

         if (singleKey) {
            return hll.cardinality();
         }

         if (merged == null) {
            merged = new HyperLogLog();
         }
         merged.mergeWith(hll);
      }

      return merged == null ? 0 : merged.cardinality();
   }
}
