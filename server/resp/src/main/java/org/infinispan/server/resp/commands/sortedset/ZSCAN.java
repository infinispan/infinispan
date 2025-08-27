package org.infinispan.server.resp.commands.sortedset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.core.iteration.IterationInitializationContext;
import org.infinispan.server.core.iteration.IterationManager;
import org.infinispan.server.core.iteration.list.ListIterationInitializationContext;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.iteration.BaseIterationCommand;

/**
 * ZSCAN
 *
 * @see <a href="https://redis.io/commands/zscan/">ZSCAN</a>
 * @see <a href="https://redis.io/commands/scan/">SCAN</a>
 * @since 15.0
 */
public class ZSCAN extends BaseIterationCommand {

   public ZSCAN() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SORTEDSET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   protected IterationManager retrieveIterationManager(Resp3Handler handler) {
      return handler.respServer().getDataStructureIterationManager();
   }

   @Override
   protected CompletionStage<IterationInitializationContext> initializeIteration(Resp3Handler handler, List<byte[]> arguments) {
      EmbeddedMultimapSortedSetCache<byte[], byte[]> multimap = handler.getSortedSeMultimap();
      return multimap.get(arguments.get(0)).thenApply(bucket -> {
         if (bucket == null) return null;
         return ListIterationInitializationContext.withSource(map(bucket));
      });
   }

   private static List<Map.Entry<Object, Object>> map(Collection<ScoredValue<byte[]>> scoredValues) {

      List<Map.Entry<Object, Object>> elements = new ArrayList<>();
      Iterator<ScoredValue<byte[]>> ite = scoredValues.iterator();
      while (ite.hasNext()) {
         ScoredValue<byte[]> item = ite.next();
         Map.Entry<Object, Object> entry = Map.entry(item.getValue(), ArgumentUtils.toByteArray(item.score()));
         elements.add(entry);
      }
      return elements;
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
         output.add((byte[]) e.getValue());
      }
      return output;
   }
}
