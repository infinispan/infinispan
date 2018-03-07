package org.infinispan.stream.impl.termop.primitive;

import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link DoubleStream}.
 * @param <Original> original stream type
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapDoubleOperation<Original, K> extends AbstractForEachOperation<Original, K, Double, DoubleStream> {
   private final DoubleConsumer consumer;

   public ForEachFlatMapDoubleOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize,
         DoubleConsumer consumer) {
      super(intermediateOperations, supplier, toKeyFunction, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Double> list) {
      list.forEach(consumer::accept);
   }

   @Override
   protected void handleStreamForEach(DoubleStream stream, List<Double> list) {
      stream.forEach(list::add);
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
