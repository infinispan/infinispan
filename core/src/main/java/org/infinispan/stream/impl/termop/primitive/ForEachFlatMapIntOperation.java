package org.infinispan.stream.impl.termop.primitive;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link IntStream}.
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapIntOperation<K> extends AbstractForEachOperation<K, Integer, IntStream> {
   private final IntConsumer consumer;

   public ForEachFlatMapIntOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, IntConsumer consumer) {
      super(intermediateOperations, supplier, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Integer> list) {
      list.forEach(consumer::accept);
   }

   @Override
   protected void handleStreamForEach(IntStream stream, List<Integer> list) {
      stream.forEach(list::add);
   }

   public IntConsumer getConsumer() {
      return consumer;
   }
}
