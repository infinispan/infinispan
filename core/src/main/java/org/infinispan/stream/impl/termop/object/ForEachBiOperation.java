package org.infinispan.stream.impl.termop.object;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal operation that handles for each where no map operations are defined
 * @param <K> key type of the supplied stream
 * @param <V> resulting value type
 */
public class ForEachBiOperation<K, V> extends AbstractForEachOperation<K, V, Stream<V>> {
   private final BiConsumer<Cache<K, ?>, ? super V> consumer;
   private transient Cache<K, ?> cache;

   public ForEachBiOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, BiConsumer<Cache<K, ?>, ? super V> consumer) {
      super(intermediateOperations, supplier, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<V> list) {
      list.forEach(e -> consumer.accept(cache, e));
   }

   @Override
   protected void handleStreamForEach(Stream<V> stream, List<V> list) {
      stream.forEach(list::add);
   }

   public BiConsumer<Cache<K, ?>, ? super V> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      super.handleInjection(registry);
      cache = registry.getComponent(Cache.class);
   }
}
