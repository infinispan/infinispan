package org.infinispan.stream.impl.termop.object;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal operation that handles for each where no map operations are defined
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 * @param <V> resulting value type
 */
public class ForEachBiOperation<Original, K, V> extends AbstractForEachOperation<Original, K, V, Stream<V>> {
   private final BiConsumer<Cache<K, ?>, ? super V> consumer;
   private transient Cache<K, ?> cache;

   public ForEachBiOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         BiConsumer<Cache<K, ?>, ? super V> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
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
      registry.wireDependencies(consumer);
      cache = registry.getComponent(Cache.class);
   }
}
