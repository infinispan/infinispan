package org.infinispan.stream.impl.termop.primitive;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link DoubleStream}.
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapDoubleOperation<K> extends AbstractForEachOperation<K, Double, DoubleStream> {
   private final DoubleConsumer consumer;

   public ForEachFlatMapDoubleOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, DoubleConsumer consumer) {
      super(intermediateOperations, supplier, batchSize);
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
}
