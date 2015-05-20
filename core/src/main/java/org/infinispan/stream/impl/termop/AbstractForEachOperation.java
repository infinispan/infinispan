package org.infinispan.stream.impl.termop;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * This is a base operation class for the use of the for each terminal operator.  This class can be used for any
 * forEach configuration, however since it relies on generics it may not be as performant as a primitive based
 * for each operation.
 * This class assumes the stream is composed of {@link java.util.Map.Entry} instances where the key is typed the same
 * as defined K type.
 * @param <K> key type of underlying stream
 * @param <V> value type of transformed stream
 * @param <S> type of the transformed stream
 */
public abstract class AbstractForEachOperation<K, V, S extends BaseStream<V, S>> extends BaseTerminalOperation
        implements KeyTrackingTerminalOperation<K, V, K> {
   private final int batchSize;

   public AbstractForEachOperation(Iterable<IntermediateOperation> intermediateOperations,
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
       * This is for rehash only! {@link SingleRunOperation} should always be used for non rehash for each
       */
      throw new UnsupportedOperationException();
   }

   protected abstract void handleList(List<V> list);

   protected abstract void handleStreamForEach(S stream, List<V> list);

   @Override
   public Collection<CacheEntry<K, K>> performOperationRehashAware(
           IntermediateCollector<Collection<CacheEntry<K, K>>> response) {
      // We only support sequential streams for iterator rehash aware
      BaseStream<?, ?> stream = supplier.get().sequential();

      List<CacheEntry<K, K>> collectedValues = new ArrayList(batchSize);

      List<V> currentList = new ArrayList<>();
      Object[] currentKey = new Object[1];
      stream = ((Stream<Map.Entry<K, ?>>) stream).peek(e -> {
         if (!currentList.isEmpty()) {
            collectedValues.add(new ImmortalCacheEntry(currentKey[0], currentKey[0]));
            if (collectedValues.size() >= batchSize) {
               handleList(currentList);
               response.sendDataResonse(collectedValues);
               collectedValues.clear();
               currentList.clear();
            }
         }
         currentKey[0] = e.getKey();
      });
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      S convertedStream = ((S) stream);
      // We rely on the fact that iterator processes 1 entry at a time
      handleStreamForEach(convertedStream, currentList);
      if (!currentList.isEmpty()) {
         handleList(currentList);
         collectedValues.add(new ImmortalCacheEntry(currentKey[0], currentKey[0]));
      }
      return collectedValues;
   }

   public int getBatchSize() {
      return batchSize;
   }
}
