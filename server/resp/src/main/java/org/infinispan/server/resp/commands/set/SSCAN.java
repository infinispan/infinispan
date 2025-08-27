package org.infinispan.server.resp.commands.set;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.core.iteration.IterationInitializationContext;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.core.iteration.map.MapIterationInitializationContext;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

/**
 * SSCAN
 *
 * @author Vittorio Rigamonti
 * @see <a href="https://redis.io/commands/sscan/">SSCAN</a>
 * @since 15.0
 */
public class SSCAN extends BaseIterationCommand {
   public SSCAN() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getDataStructureIterationManager();
   }

   @Override
   protected CompletionStage<IterationInitializationContext> initializeIteration(Resp3Handler handler,
                                                                                 List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> multimap = handler.getEmbeddedSetCache();
      return multimap.get(arguments.get(0)).thenApply(entry -> {
         if (entry == null) {
            return null;
         }
         return MapIterationInitializationContext.withSource(setToMap(entry));
      });
   }

   private Map<byte[], byte[]> setToMap(SetBucket<byte[]> entry) {
      return entry.toList().stream().collect(Collectors.toMap(v -> v, v -> Util.EMPTY_BYTE_ARRAY));
   }

   @Override
   protected String cursor(List<byte[]> raw) {
      return new String(raw.get(1), StandardCharsets.US_ASCII);
   }

   @Override
   protected List<byte[]> writeResponse(List<CacheEntry> response) {
      List<byte[]> output = new ArrayList<>(response.size());
      for (CacheEntry<?, ?> e : response) {
         output.add((byte[]) e.getKey());
      }
      return output;
   }
}
