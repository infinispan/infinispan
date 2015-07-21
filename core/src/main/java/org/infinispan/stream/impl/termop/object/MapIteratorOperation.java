package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.BaseTerminalOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles an iterator when a map intermediate operation was performed on
 * the stream.  This is important due to the fact that we need to figure out the keys that map to each entry still.
 * This class assumes the stream is composed of {@link java.util.Map.Entry} instances where the key is typed the same
 * as defined K type.
 * @param <K> key type
 * @param <V> unused type
 * @param <V2> resulting type from the operation
 */
public class MapIteratorOperation<K, V, V2> extends BaseTerminalOperation implements KeyTrackingTerminalOperation<K, V,
        V2> {
   protected final int batchSize;

   public MapIteratorOperation(Iterable<IntermediateOperation> intermediateOperations,
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
   public Collection<CacheEntry<K, V2>> performOperationRehashAware(
           IntermediateCollector<Collection<CacheEntry<K, V2>>> response) {
      // We only support sequential streams for iterator rehash aware
      BaseStream<?, ?> stream = supplier.get().sequential();

      List<CacheEntry<K, V2>> collectedValues = new ArrayList(batchSize);

      Object[] currentKey = new Object[1];
      stream = ((Stream<Map.Entry<K, ?>>) stream).peek(
              e -> currentKey[0] = e.getKey());
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      Stream<V> convertedStream = ((Stream<V>)stream);
      // We rely on the fact that iterator processes 1 entry at a time
      convertedStream.forEach(v -> {
         // TODO: do we care about metadata in this case?
         collectedValues.add(new ImmortalCacheEntry(currentKey[0], v));
         if (collectedValues.size() >= batchSize) {
            response.sendDataResonse(collectedValues);
            collectedValues.clear();
         }
      });
      return collectedValues;
   }

   public int getBatchSize() {
      return batchSize;
   }
}
