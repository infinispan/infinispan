package org.infinispan.stream.impl.termop.primitive;

import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Terminal rehash aware operation that handles for each where no flat map operations are defined on a
 * {@link IntStream}. Note this means it is an implied map intermediate operation.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachObjIntOperation<Original, K> extends AbstractForEachIntOperation<Original, K> {
   private final ObjIntConsumer<Cache<K, ?>> consumer;
   private transient Cache<K, ?> cache;

   public ForEachObjIntOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         ObjIntConsumer<Cache<K, ?>> consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleArray(int[] array, int size) {
      for (int i = 0; i < size; ++i) {
         consumer.accept(cache, array[i]);
      }
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
