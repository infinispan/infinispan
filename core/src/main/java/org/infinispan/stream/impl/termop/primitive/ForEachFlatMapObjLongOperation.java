package org.infinispan.stream.impl.termop.primitive;

import java.util.List;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link LongStream}.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapObjLongOperation<Original, K> extends AbstractForEachOperation<Original, K, Long, LongStream> {
   private final ObjLongConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachFlatMapObjLongOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         ObjLongConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Long> list) {
      list.forEach(d -> consumer.accept(cache, d));
   }

   @Override
   protected void handleStreamForEach(LongStream stream, List<Long> list) {
      stream.forEach(list::add);
   }

   public ObjLongConsumer<Cache<K, ?>> getConsumer() {
      return consumer;
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      super.handleInjection(registry);
      cache = registry.getComponent(Cache.class);
   }
}
