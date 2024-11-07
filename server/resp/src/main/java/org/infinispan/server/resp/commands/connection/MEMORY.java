package org.infinispan.server.resp.commands.connection;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * MEMORY
 *
 * @see <a href="https://redis.io/commands/memory/">MEMORY</a>
 * @since 15.0
 */
public class MEMORY extends RespCommand implements Resp3Command {

   private static final JavaObjectSerializer<Map<String, Number>> SERIALIZER = (res, writer) -> {
      writer.writeNumericPrefix(RespConstants.MAP, res.size());
      for (Map.Entry<String, Number> entry : res.entrySet()) {
         writer.simpleString(entry.getKey());

         Number v = entry.getValue();
         if (v instanceof Double) {
            writer.doubles(v);
         } else {
            writer.integers(v);
         }
      }
   };

   public MEMORY() {
      super(-2, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      String subcommand = new String(arguments.get(0), StandardCharsets.US_ASCII).toUpperCase();
      switch (subcommand) {
         case "STATS":
            // Map mixes integer and double values.
            handler.writer().write(generateMemoryStats(), SERIALIZER);
            break;
         case "USAGE":
            if (arguments.size() < 2) {
               handler.writer().wrongArgumentCount(this);
               return handler.myStage();
            } else {
               byte[] key = arguments.get(1);
               CompletionStage<CacheEntry<byte[], Object>> cs = handler.typedCache(null).getCacheEntryAsync(key);
               CompletionStage<Long> cs1 = cs
                     .thenApply(e -> MemoryEntrySizeUtils.calculateSize(key, (InternalCacheEntry<byte[], Object>) e));
               return handler.stageToReturn(cs1, ctx, ResponseWriter.INTEGER);
            }
         case "DOCTOR":
         case "MALLOC-STATS":
         case "PURGE":
            handler.writer().customError("module loading/unloading unsupported");
            break;
      }
      return handler.myStage();
   }

   private static Map<String, Number> generateMemoryStats() {
      Map<String, Number> collector = new HashMap<>();
      collector.put("peak.allocated", 0);
      collector.put("total.allocated", 0);
      collector.put("startup.allocated", 0);
      collector.put("replication.backlog", 0);
      collector.put("clients.slaves", 0);
      collector.put("clients.normal", 0);
      collector.put("cluster.links", 0);
      collector.put("aof.buffer", 0);
      collector.put("lua.caches", 0);
      collector.put("functions.caches", 0);
      collector.put("overhead.total", 0);
      collector.put("keys.count", 0);
      collector.put("keys.bytes-per-key", 0);
      collector.put("dataset.bytes", 0);
      collector.put("dataset.percentage", 0d);
      collector.put("peak.percentage", 0d);
      collector.put("allocator.allocated", 0);
      collector.put("allocator.active", 0);
      collector.put("allocator.resident", 0);
      collector.put("allocator-fragmentation.ratio", 0d);
      collector.put("allocator-fragmentation.bytes", 0);
      collector.put("allocator-rss.ratio", 0d);
      collector.put("allocator-rss.bytes", 0);
      collector.put("rss-overhead.ratio", 0d);
      collector.put("rss-overhead.bytes", 0);
      collector.put("fragmentation", 0d);
      collector.put("fragmentation.bytes", 0);
      return collector;
   }
}
