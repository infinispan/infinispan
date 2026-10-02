package org.infinispan.commons.util;

import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.ToIntFunction;

/**
 * A counting variant of {@link BloomFilter}. Instead of a single bit, every position holds an unsigned 8 bit counter
 * which is incremented when a value is added and decremented when a value is removed. This allows values to be
 * removed from the filter, something a plain bloom filter cannot support, at the cost of using one byte per position
 * instead of one bit.
 * <p>
 * A value may only be removed from the filter if it was added to it beforehand. Removing a value that was never
 * added decrements counters that belong to other values and can therefore introduce false negatives, which a bloom
 * filter must never produce. To make such a mistake harmless {@link #removeFromFilter(Object)} is a no-op unless all
 * the counters of the given value are non zero, but the caller is still responsible for never removing a value more
 * often than it was added.
 * <p>
 * Counters saturate at {@value #MAX_COUNT}. A saturated counter is never decremented again, as its real value is no
 * longer known. This can only cause additional false positives, never a false negative.
 * <p>
 * Instances support a single writer with any number of concurrent readers: {@link #possiblyPresent(Object)} may be
 * invoked from any thread, while all the mutative methods must be confined to a single thread.
 *
 * @since 16.3
 */
public class CountingBloomFilter<E> {
   /**
    * The highest value a single counter can hold.
    */
   public static final int MAX_COUNT = 0xFF;

   private static final int COUNTERS_PER_INT = 4;
   private static final int COUNTER_SHIFT = 2;

   private final int counterCount;
   private final Iterable<ToIntFunction<? super E>> hashFunctions;
   private final AtomicIntegerArray counters;

   CountingBloomFilter(int counterCount, Iterable<ToIntFunction<? super E>> hashFunctions) {
      if (counterCount <= 0) {
         throw new IllegalArgumentException("Number of counters must be positive, received " + counterCount);
      }
      this.counterCount = counterCount;
      this.hashFunctions = hashFunctions;
      this.counters = new AtomicIntegerArray((counterCount + COUNTERS_PER_INT - 1) >>> COUNTER_SHIFT);
   }

   public static <E> CountingBloomFilter<E> createFilter(int counterCount, Iterable<ToIntFunction<? super E>> hashFunctions) {
      return new CountingBloomFilter<>(counterCount, hashFunctions);
   }

   /**
    * Adds a value to the filter, incrementing the counter of every position the value hashes to. This method returns
    * {@code true} if any of those counters was previously zero, meaning this value was for sure not present before.
    *
    * @param value the value to add to the filter
    * @return whether the value was for sure not present before
    */
   public boolean addToFilter(E value) {
      boolean wasAbsent = false;
      for (ToIntFunction<? super E> function : hashFunctions) {
         wasAbsent |= increment(position(function, value));
      }
      return wasAbsent;
   }

   /**
    * Removes a value from the filter, decrementing the counter of every position the value hashes to. If the value is
    * not {@link #possiblyPresent(Object)} nothing is changed, as decrementing in that case could only corrupt the
    * counters of other values.
    *
    * @param value the value to remove from the filter
    * @return whether the filter was actually updated
    */
   public boolean removeFromFilter(E value) {
      if (!possiblyPresent(value)) {
         return false;
      }
      for (ToIntFunction<? super E> function : hashFunctions) {
         decrement(position(function, value));
      }
      return true;
   }

   /**
    * Returns {@code true} if the element might be present, {@code false} if the value was for sure not present.
    *
    * @param value the value to check for
    * @return whether this value may be present or for sure not
    */
   public boolean possiblyPresent(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         if (count(position(function, value)) == 0) {
            return false;
         }
      }
      return true;
   }

   /**
    * Resets every counter back to zero, emptying the filter.
    */
   public void clear() {
      for (int i = 0; i < counters.length(); ++i) {
         counters.set(i, 0);
      }
   }

   /**
    * Clears all current counters and sets the counter of every position present in the provided {@link IntSet} to one.
    * Positions that are outside of this filter are ignored.
    * <p>
    * Note that the resulting filter has lost the multiplicity of the values it holds, so a subsequent
    * {@link #removeFromFilter(Object)} may remove more than the value it was given. It is only meant to be used when
    * the caller replaces the entire contents of the filter and does not remove values from it afterwards.
    *
    * @param intSet the positions that should be marked as present
    */
   public void setBits(IntSet intSet) {
      clear();
      for (PrimitiveIterator.OfInt iter = intSet.iterator(); iter.hasNext(); ) {
         int position = iter.nextInt();
         if (position < counterCount) {
            increment(position);
         }
      }
   }

   /**
    * @return the positions that currently have a non zero counter
    */
   public IntSet getIntSet() {
      IntSet intSet = IntSets.mutableEmptySet(counterCount);
      for (int i = 0; i < counterCount; ++i) {
         if (count(i) != 0) {
            intSet.set(i);
         }
      }
      return intSet;
   }

   /**
    * @return the number of positions that currently have a non zero counter
    */
   public int occupiedPositions() {
      int occupied = 0;
      for (int i = 0; i < counterCount; ++i) {
         if (count(i) != 0) {
            occupied++;
         }
      }
      return occupied;
   }

   private int position(ToIntFunction<? super E> function, E value) {
      return Math.abs(function.applyAsInt(value)) % counterCount;
   }

   private int count(int position) {
      return (counters.get(position >>> COUNTER_SHIFT) >>> shift(position)) & MAX_COUNT;
   }

   private boolean increment(int position) {
      int index = position >>> COUNTER_SHIFT;
      int shift = shift(position);
      while (true) {
         int packed = counters.get(index);
         int current = (packed >>> shift) & MAX_COUNT;
         if (current == MAX_COUNT) {
            // Saturated, we can no longer track how many values map to this position
            return false;
         }
         if (counters.compareAndSet(index, packed, packed + (1 << shift))) {
            return current == 0;
         }
      }
   }

   private void decrement(int position) {
      int index = position >>> COUNTER_SHIFT;
      int shift = shift(position);
      while (true) {
         int packed = counters.get(index);
         int current = (packed >>> shift) & MAX_COUNT;
         if (current == 0 || current == MAX_COUNT) {
            // A saturated counter can never be lowered again without risking a false negative
            return;
         }
         if (counters.compareAndSet(index, packed, packed - (1 << shift))) {
            return;
         }
      }
   }

   private static int shift(int position) {
      return (position & (COUNTERS_PER_INT - 1)) << 3;
   }

   @Override
   public String toString() {
      return "CountingBloomFilter{" +
            "counterCount=" + counterCount +
            ", occupiedPositions=" + occupiedPositions() +
            '}';
   }
}
