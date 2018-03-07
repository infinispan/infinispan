package org.infinispan.stream.impl.termop.primitive;

import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Terminal rehash aware operation that handles for each where no flat map operations are defined on a
 * {@link LongStream}. Note this means it is an implied map intermediate operation.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachObjLongOperation<Original, K> extends AbstractForEachLongOperation<Original, K> {
   private final ObjLongConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachObjLongOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         ObjLongConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleArray(long[] array, int size) {
      for (int i = 0; i < size; ++i) {
         consumer.accept(cache, array[i]);
      }
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
