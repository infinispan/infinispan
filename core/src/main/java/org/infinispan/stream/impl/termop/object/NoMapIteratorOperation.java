package org.infinispan.stream.impl.termop.object;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * Terminal rehash aware operation that handles an iterator when no flat map or map intermediate operation was performed
 * on the stream.  This is important due to the fact that we can just return the entries as is and the client can sort
 * out what is the key and what isn't.
 * @param <K> key type
 * @param <V> resulting value type
 */
public class NoMapIteratorOperation<K, V> extends MapIteratorOperation<K, V, V> {
   public NoMapIteratorOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<CacheEntry>> supplier, int batchSize) {
      super(intermediateOperations, supplier, batchSize);
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      // TODO: stop this early
      return true;
   }

   @Override
   public List<V> performOperation(IntermediateCollector<Collection<V>> response) {
      BaseStream<?, ?> stream = supplier.get();
      for (IntermediateOperation intOp : intermediateOperations) {
         stream = intOp.perform(stream);
      }

      Stream<V> convertedStream = ((Stream<V>)stream);
      return actualPerformOperation(response, convertedStream);
   }

   private <R> List<R> actualPerformOperation(IntermediateCollector<Collection<R>> response, Stream<R> stream) {
      BiConsumer<List<R>, R> accumulator = (l, e) -> {
         l.add(e);
         if (l.size() >= batchSize) {
            response.sendDataResonse(l);
            l.clear();
         }
      };

      // We use collect instead of forEach due to the fact that forEach would require a concurrent
      // list to handle, where as collect will merge them together in a thread safe way
      List<R> list = stream.collect(ArrayList::new, accumulator, (l1, l2) -> {
         l2.forEach(e -> accumulator.accept(l1, e));
      });
      return list;
   }

   @Override
   public Collection<CacheEntry<K, V>> performOperationRehashAware(
           IntermediateCollector<Collection<CacheEntry<K, V>>> response) {
      // We only support sequential streams for iterator rehash aware
      BaseStream<?, ?> stream = supplier.get().sequential();

      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      return actualPerformOperation(response, (Stream<CacheEntry<K, V>>) stream);
   }
}
