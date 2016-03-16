package org.infinispan.stream.impl.termop.primitive;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

import java.util.List;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link IntStream}.
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapObjIntOperation<K> extends AbstractForEachOperation<K, Integer, IntStream> {
   private final ObjIntConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachFlatMapObjIntOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, ObjIntConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Integer> list) {
      list.forEach(d -> consumer.accept(cache, d));
   }

   @Override
   protected void handleStreamForEach(IntStream stream, List<Integer> list) {
      stream.forEach(list::add);
   }

   public ObjIntConsumer<Cache<K, ?>> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      super.handleInjection(registry);
      cache = registry.getComponent(Cache.class);
   }
}
