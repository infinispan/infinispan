package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Terminal operation that handles for each where no map operations are defined
 * @param <K> key type of the supplied stream
 * @param <V> resulting value type
 */
public class ForEachOperation<K, V> extends AbstractForEachOperation<K, V, Stream<V>> {
   private final Consumer<? super V> consumer;

   public ForEachOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, Consumer<? super V> consumer) {
      super(intermediateOperations, supplier, batchSize);
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
}
