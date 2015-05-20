package org.infinispan.stream.impl.termop.primitive;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles for each where no flat map operations are defined on a
 * {@link LongStream}. Note this means it is an implied map intermediate operation.
 * @param <K> key type of the supplied stream
 */
public class ForEachLongOperation<K> extends BaseTerminalOperation implements KeyTrackingTerminalOperation<K, Long, K> {
   private final int batchSize;
   private final LongConsumer consumer;

   public ForEachLongOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize, LongConsumer consumer) {
      super(intermediateOperations, supplier);
      this.batchSize = batchSize;
      this.consumer = consumer;
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      // TODO: stop this early
      return true;
   }

   @Override
   public List<Long> performOperation(IntermediateCollector<Collection<Long>> response) {
      /**
       * This is for rehash only! {@link NoMapIteratorOperation} should always be used for non rehash
       */
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<CacheEntry<K, K>> performOperationRehashAware(
           IntermediateCollector<Collection<CacheEntry<K, K>>> response) {
      // We only support sequential streams for iterator rehash aware
      BaseStream<?, ?> stream = supplier.get().sequential();

      List<CacheEntry<K, K>> collectedValues = new ArrayList(batchSize);

      long[] list = new long[batchSize];
      AtomicInteger offset = new AtomicInteger();
      Object[] currentKey = new Object[1];
      stream = ((Stream<Map.Entry<K, ?>>) stream).peek(e -> {
         if (offset.get() > 0) {
            collectedValues.add(new ImmortalCacheEntry(currentKey[0], currentKey[0]));
            if (collectedValues.size() >= batchSize) {
               for (int i = 0; i < offset.get(); ++i) {
                  consumer.accept(list[i]);
               }
               response.sendDataResonse(collectedValues);
               collectedValues.clear();
               offset.set(0);
            }
         }
         currentKey[0] = e.getKey();
      });
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      LongStream convertedStream = ((LongStream)stream);
      // We rely on the fact that iterator processes 1 entry at a time when sequential
      convertedStream.forEach(d -> list[offset.getAndIncrement()] = d);
      if (offset.get() > 0) {
         for (int i = 0; i < offset.get(); ++i) {
            consumer.accept(list[i]);
         }
         collectedValues.add(new ImmortalCacheEntry(currentKey[0], currentKey[0]));
      }
      return collectedValues;
   }

   public int getBatchSize() {
      return batchSize;
   }

   public LongConsumer getConsumer() {
      return consumer;
   }
}
