package org.infinispan.commons.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.BitSet;
import java.util.function.ToIntFunction;

/**
 * A counting variant of {@link BloomFilter} which also supports removing values.
 * <p>
 * A plain bloom filter can only ever set bits, so the only way to drop a value from it is to rebuild the whole filter
 * from the values that are left. This implementation keeps an unsigned 8 bit counter per bit instead, which makes
 * both {@link #addToFilter(Object)} and {@link #removeFromFilter(Object)} constant time at the cost of one byte per
 * bit of the filter.
 * <p>
 * The bits returned by {@link #toBitSet()} are exactly the ones a {@link BloomFilter} created with the same number of
 * bits and the same hash functions would have set for the same values, so one side of a connection can maintain a
 * counting filter while the other keeps a plain one.
 * <p>
 * Counters saturate at {@value #MAX_COUNT}: once a counter reaches that value it is never decremented again and its
 * bit stays set for the lifetime of the filter. This way the filter may report additional false positives but it
 * never reports a false negative.
 * <p>
 * Instances may be used concurrently without additional synchronization.
 *
 * @since 16.3
 */
public class CountingBloomFilter<E> {
   /**
    * Value at which a counter stops being updated.
    */
   public static final int MAX_COUNT = 255;

   private static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(byte[].class);

   private final int bitsToUse;
   private final byte[] counters;
   private final ToIntFunction<? super E>[] hashFunctions;

   CountingBloomFilter(int bitsToUse, ToIntFunction<? super E>[] hashFunctions) {
      if (bitsToUse <= 0) {
         throw new IllegalArgumentException("Number of bits must be positive, received " + bitsToUse);
      }
      this.bitsToUse = bitsToUse;
      this.counters = new byte[bitsToUse];
      this.hashFunctions = hashFunctions;
   }

   @SafeVarargs
   public static <E> CountingBloomFilter<E> createFilter(int bitsToUse, ToIntFunction<? super E>... hashFunctions) {
      return new CountingBloomFilter<>(bitsToUse, hashFunctions);
   }

   /**
    * Adds a value to the filter incrementing up to a number of counters equal to the number of hash functions. This
    * method will also return {@code true} if any of the bits were updated, meaning this value was for sure not
    * present before.
    *
    * @param value the value to add to the filter
    * @return whether the filter was actually updated
    */
   public boolean addToFilter(E value) {
      boolean setABit = false;
      for (ToIntFunction<? super E> function : hashFunctions) {
         setABit |= increment(bitFor(function, value));
      }
      return setABit;
   }

   /**
    * Removes a value that was previously added to the filter, decrementing up to a number of counters equal to the
    * number of hash functions, and returns how many of those counters dropped to zero.
    * <p>
    * The returned value is the number of bits that this removal cleared, which is zero whenever the values still in
    * the filter happen to use all the bits of the removed value.
    * <p>
    * As with any counting filter, a value may only be removed as many times as it was added. Removing a value that
    * is not in the filter decrements counters that other values rely on and those values may then be reported as
    * absent. Counters are never decremented below zero, so removing from an empty filter is a no op.
    *
    * @param value the value to remove from the filter
    * @return the number of bits that are no longer set because of this removal
    */
   public int removeFromFilter(E value) {
      int clearedBits = 0;
      for (ToIntFunction<? super E> function : hashFunctions) {
         clearedBits += decrement(bitFor(function, value));
      }
      return clearedBits;
   }

   /**
    * Returns {@code true} if the element might be present, {@code false} if the value was for sure not present.
    *
    * @param value the value to check for
    * @return whether this value may be present or for sure not
    */
   public boolean possiblyPresent(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         if (count(bitFor(function, value)) == 0) {
            return false;
         }
      }
      return true;
   }

   /**
    * Removes all the values from this filter.
    */
   public void clear() {
      for (int i = 0; i < counters.length; ++i) {
         COUNTERS.setVolatile(counters, i, (byte) 0);
      }
   }

   /**
    * Returns the bits that are currently set, that is the ones whose counter is not zero.
    *
    * @return the set bits
    */
   public IntSet getIntSet() {
      return IntSets.immutableSet(IntSets.from(toBitSet()));
   }

   /**
    * Same as {@link #getIntSet()} but returns the bits already encoded the same way {@link IntSet#toBitSet()} does.
    *
    * @return the set bits as a bit set
    */
   public byte[] toBitSet() {
      BitSet bitSet = new BitSet(bitsToUse);
      for (int i = 0; i < bitsToUse; ++i) {
         if (count(i) != 0) {
            bitSet.set(i);
         }
      }
      return bitSet.toByteArray();
   }

   private int bitFor(ToIntFunction<? super E> function, E value) {
      // Has to match BloomFilter so that both can be used on either side of a connection
      int hashResult = function.applyAsInt(value);
      return (hashResult == Integer.MIN_VALUE ? 0 : Math.abs(hashResult)) % bitsToUse;
   }

   private int count(int bit) {
      return Byte.toUnsignedInt((byte) COUNTERS.getVolatile(counters, bit));
   }

   /**
    * @return whether the bit was not set before
    */
   private boolean increment(int bit) {
      while (true) {
         byte witness = (byte) COUNTERS.getVolatile(counters, bit);
         int count = Byte.toUnsignedInt(witness);
         if (count == MAX_COUNT) {
            return false;
         }
         if (COUNTERS.compareAndSet(counters, bit, witness, (byte) (count + 1))) {
            return count == 0;
         }
      }
   }

   /**
    * @return 1 if the bit is no longer set, 0 otherwise
    */
   private int decrement(int bit) {
      while (true) {
         byte witness = (byte) COUNTERS.getVolatile(counters, bit);
         int count = Byte.toUnsignedInt(witness);
         if (count == 0 || count == MAX_COUNT) {
            return 0;
         }
         if (COUNTERS.compareAndSet(counters, bit, witness, (byte) (count - 1))) {
            return count == 1 ? 1 : 0;
         }
      }
   }

   @Override
   public String toString() {
      return "CountingBloomFilter{" +
            "bitsToUse=" + bitsToUse +
            ", intSet=" + getIntSet() +
            '}';
   }
}
