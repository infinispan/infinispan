package org.infinispan.stream.impl.termop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import org.infinispan.commons.util.ByRef;
import org.infinispan.stream.impl.KeyTrackingTerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * This is a base operation class for the use of the for each terminal operator.  This class can be used for any
 * forEach configuration, however since it relies on generics it may not be as performant as a primitive based
 * for each operation.
 * This class assumes the stream is composed of {@link java.util.Map.Entry} instances where the key is typed the same
 * as defined K type.
 * @param <Original> original stream type
 * @param <K> key type of underlying stream
 * @param <V> value type of transformed stream
 * @param <S> type of the transformed stream
 */
public abstract class AbstractForEachOperation<Original, K, V, S extends BaseStream<V, S>> extends BaseTerminalOperation<Original>
        implements KeyTrackingTerminalOperation<Original, K, V> {
   private final int batchSize;
   private final Function<? super Original, ? extends K> toKeyFunction;

   public AbstractForEachOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<Stream<Original>> supplier, Function<? super Original, ? extends K> toKeyFunction, int batchSize) {
      super(intermediateOperations, supplier);
      this.batchSize = batchSize;
      this.toKeyFunction = toKeyFunction;
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
   public Collection<K> performForEachOperation(IntermediateCollector<Collection<K>> response) {
      // We only support sequential streams for forEach rehash aware
      Stream<Original> originalStream = supplier.get().sequential();
      List<K> collectedValues = new ArrayList<>(batchSize);

      List<V> currentList = new ArrayList<>();
      ByRef<K> currentKey = new ByRef<>(null);
      originalStream = originalStream.peek(e -> {
         if (!currentList.isEmpty()) {
            collectedValues.add(currentKey.get());
            if (collectedValues.size() >= batchSize) {
               handleList(currentList);
               response.sendDataResonse(collectedValues);
               collectedValues.clear();
               currentList.clear();
            }
         }
         currentKey.set(toKeyFunction.apply(e));
      });
      BaseStream<?, ?> stream = originalStream;
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }

      S convertedStream = ((S) stream);
      // We rely on the fact that forEach processes 1 entry at a time
      handleStreamForEach(convertedStream, currentList);
      if (!currentList.isEmpty()) {
         handleList(currentList);
         collectedValues.add(currentKey.get());
      }
      return collectedValues;
   }

   public Function<? super Original, ? extends K> getToKeyFunction() {
      return toKeyFunction;
   }

   public int getBatchSize() {
      return batchSize;
   }
}
