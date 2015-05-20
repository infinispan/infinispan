package org.infinispan.stream.impl.termop.primitive;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.AbstractForEachOperation;

import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles for each where flat map operation is performed on a
 * {@link LongStream}.
 * @param <K> key type of the supplied stream
 */
public class ForEachFlatMapLongOperation<K> extends AbstractForEachOperation<K, Long, LongStream> {
   private final LongConsumer consumer;

   public ForEachFlatMapLongOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, LongConsumer consumer) {
      super(intermediateOperations, supplier, batchSize);
      this.consumer = consumer;
   }

   @Override
   protected void handleList(List<Long> list) {
      list.forEach(consumer::accept);
   }

   @Override
   protected void handleStreamForEach(LongStream stream, List<Long> list) {
      stream.forEach(list::add);
   }

   public LongConsumer getConsumer() {
      return consumer;
   }
}
