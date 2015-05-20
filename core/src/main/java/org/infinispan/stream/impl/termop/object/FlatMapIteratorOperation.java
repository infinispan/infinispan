package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles an iterator when a flat map intermediate operation was performed on
 * the stream.  This is important due to the fact that we need to track what keys have been processed for the iterator.
 * Since flat map can produce multiple values for the same key we need to handle that special so we can guarantee
 * we are returning the keys properly and a rehash wouldn't lose some.
 * This class assumes the stream is composed of {@link java.util.Map.Entry} instances where the key is typed the same
 * as defined K type.
 * @param <K> key type
 * @param <V> resulting value type
 */
public class FlatMapIteratorOperation<K, V> extends BaseTerminalOperation implements KeyTrackingTerminalOperation<K, V,
        Collection<V>> {
   private final int batchSize;

   public FlatMapIteratorOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize) {
      super(intermediateOperations, supplier);
      this.batchSize = batchSize;
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      // TODO: stop this early
      return true;
   }

   @Override
   public List<V> performOperation(IntermediateCollector<Collection<V>> response) {
      /**
       * This is for rehash only! {@link NoMapIteratorOperation} should always be used for non rehash
       */
      throw new UnsupportedOperationException();
   }

   @Override
   public Collection<CacheEntry<K, Collection<V>>> performOperationRehashAware(
           IntermediateCollector<Collection<CacheEntry<K, Collection<V>>>> response) {
      // We only support sequential streams for iterator rehash aware
      BaseStream<?, ?> stream = supplier.get().sequential();

      List<CacheEntry<K, Collection<V>>> collectedValues = new ArrayList(batchSize);

      List<V>[] currentList = new List[1];
      currentList[0] = new ArrayList<>();
      Object[] currentKey = new Object[1];
      stream = ((Stream<Map.Entry<K, ?>>) stream).peek(e -> {
         List<V> list = currentList[0];
         if (!list.isEmpty()) {
            collectedValues.add(new ImmortalCacheEntry((K)currentKey[0], list));
            if (collectedValues.size() >= batchSize) {
               response.sendDataResonse(collectedValues);
               collectedValues.clear();
               list.clear();
            } else {
               currentList[0] = new ArrayList<V>(list.size());
            }
         }
         currentKey[0] = e.getKey();
      });
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      Stream<V> convertedStream = ((Stream<V>)stream);
      // We rely on the fact that iterator processes 1 entry at a time
      convertedStream.forEach(v -> {
         currentList[0].add(v);
      });
      List<V> lastList = currentList[0];
      if (lastList != null && !lastList.isEmpty()) {
         collectedValues.add(new ImmortalCacheEntry(currentKey[0], lastList));
      }
      return collectedValues;
   }

   public int getBatchSize() {
      return batchSize;
   }
}
