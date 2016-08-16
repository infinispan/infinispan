package org.infinispan.stream.impl.termop.primitive;

import java.util.List;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link DoubleStream}.
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapObjDoubleOperation<K> extends AbstractForEachOperation<K, Double, DoubleStream> {
   private final ObjDoubleConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachFlatMapObjDoubleOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, ObjDoubleConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Double> list) {
      list.forEach(d -> consumer.accept(cache, d));
   }

   @Override
   protected void handleStreamForEach(DoubleStream stream, List<Double> list) {
      stream.forEach(list::add);
   }

   public ObjDoubleConsumer<Cache<K, ?>> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      super.handleInjection(registry);
      cache = registry.getComponent(Cache.class);
   }
}
