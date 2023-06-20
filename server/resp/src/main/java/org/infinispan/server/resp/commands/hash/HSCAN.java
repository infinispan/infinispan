package org.infinispan.server.resp.commands.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.iteration.IterationInitializationContext;
import org.infinispan.server.iteration.IterationManager;
import org.infinispan.server.iteration.map.MapIterationInitializationContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

/**
 * `<code>HSCAN key cursor [MATCH pattern] [COUNT count]</code>` command.
 * <p>
 * Scan the key-value properties of the hash stored under <code>key</code>.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hscan/">Redis Documentation</a>
 * @author José Bolina
 */
public class HSCAN extends BaseIterationCommand {

   public HSCAN() {
      super(-3, 1, 1, 1);
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getDataStructureIterationManager();
   }

   @Override
   protected CompletionStage<IterationInitializationContext> initializeIteration(Resp3Handler handler, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      return multimap.get(arguments.get(0)).thenApply(entry -> {
         if (entry == null) return null;

         return MapIterationInitializationContext.withSource(entry);
      });
   }

   @Override
   protected String cursor(List<byte[]> raw) {
      return new String(raw.get(1), StandardCharsets.US_ASCII);
   }

   @Override
   protected List<byte[]> writeResponse(List<CacheEntry> response) {
      List<byte[]> output = new ArrayList<>(2 * response.size());
      for (CacheEntry<?, ?> e : response) {
         output.add((byte[]) e.getKey());
         output.add((byte[]) e.getValue());
      }
      return output;
   }
}
