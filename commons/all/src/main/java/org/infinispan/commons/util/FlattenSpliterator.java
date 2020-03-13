package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Composes an array of Collections into a spliterator. This spliterator will only split up to the collection and will
 * not split the spliterator from the collection itself.
 * @author wburns
 * @since 9.3
 */
public class FlattenSpliterator<E> implements Spliterator<E> {
   private final IntFunction<Collection<E>> toCollection;
   private final int length;
   private int index;        // current index, modified on advance/split
   private final int fence;  // one past last index
   private final int characteristics;

   private Spliterator<E> currentSpliterator;

   public FlattenSpliterator(IntFunction<Collection<E>> toCollection, int length, int additionalCharacteristics) {
      this(toCollection, length, 0, length, additionalCharacteristics);
   }

   private FlattenSpliterator(IntFunction<Collection<E>> toCollection, int length, int index, int fence, int characteristics) {
      this.toCollection = toCollection;
      this.length = length;
      if (index < 0) {
         throw new IllegalArgumentException("Index " + index + " was less than 0!");
      }
      this.index = index;
      this.fence = fence;
      this.characteristics = characteristics;
   }

   @Override
   public boolean tryAdvance(Consumer<? super E> action) {
      boolean advanced = false;
      // If the current spliterator is null or can't advance the current action try the next one
      while ((currentSpliterator == null || !(advanced = currentSpliterator.tryAdvance(action))) && index < fence) {
         currentSpliterator = toCollection.apply(index++).spliterator();
      }
      return advanced;
   }

   @Override
   public void forEachRemaining(Consumer<? super E> action) {
      for (; index < fence; ++index) {
         toCollection.apply(index).spliterator().forEachRemaining(action);
      }
   }

   @Override
   public Spliterator<E> trySplit() {
      int lo = index, mid = (lo + fence) >>> 1;
      return (lo >= mid)
            ? null
            : new FlattenSpliterator<>(toCollection, length, lo, index = mid, characteristics);
   }

   @Override
   public long estimateSize() {
      long estimate = 0;
      if (currentSpliterator != null) {
         estimate += currentSpliterator.estimateSize();
      }
      if (estimate == Long.MAX_VALUE) {
         return estimate;
      }
      for (int i = index; i < fence; ++i) {
         estimate += toCollection.apply(i).size();
         if (estimate < 0) {
            return Long.MAX_VALUE;
         }
      }
      return estimate;
   }

   @Override
   public int characteristics() {
      return characteristics;
   }
}
