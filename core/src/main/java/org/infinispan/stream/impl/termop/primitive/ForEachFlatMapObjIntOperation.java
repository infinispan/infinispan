package org.infinispan.stream.impl.termop.primitive;

import java.util.List;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link IntStream}.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapObjIntOperation<Original, K> extends AbstractForEachOperation<Original, K, Integer, IntStream> {
   private final ObjIntConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachFlatMapObjIntOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         ObjIntConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
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
