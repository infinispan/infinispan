package org.infinispan.stream.impl.termop.object;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal operation that handles for each where no map operations are defined
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 * @param <V> resulting value type
 */
public class ForEachOperation<Original, K, V> extends AbstractForEachOperation<Original, K, V, Stream<V>> {
   private final Consumer<? super V> consumer;

   public ForEachOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         Consumer<? super V> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<V> list) {
      list.forEach(consumer);
   }

   @Override
   protected void handleStreamForEach(Stream<V> stream, List<V> list) {
      stream.forEach(list::add);
   }

   public Consumer<? super V> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      super.handleInjection(registry);
      if (consumer instanceof CacheAware) {
         ((CacheAware) consumer).injectCache(registry.getComponent(Cache.class));
      }
   }
}
