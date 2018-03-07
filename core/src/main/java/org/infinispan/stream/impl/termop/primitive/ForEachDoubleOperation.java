package org.infinispan.stream.impl.termop.primitive;

import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Terminal rehash aware operation that handles for each where no flat map operations are defined on a
 * {@link DoubleStream}. Note this means it is an implied map intermediate operation.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachDoubleOperation<Original, K> extends AbstractForEachDoubleOperation<Original, K> {
   private final DoubleConsumer consumer;

   public ForEachDoubleOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         DoubleConsumer consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleArray(double[] array, int size) {
      for (int i = 0; i < size; ++i) {
         consumer.accept(array[i]);
      }
   }

   public DoubleConsumer getConsumer() {
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
