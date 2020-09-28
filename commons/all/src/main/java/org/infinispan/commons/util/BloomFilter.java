package org.infinispan.commons.util;

import java.util.function.ToIntFunction;

public class BloomFilter<E> {
   private final int bitsToUse;
   private final IntSet intSet;
   private final Iterable<ToIntFunction<? super E>> hashFunctions;

   BloomFilter(int bitsToUse, IntSet intSet, Iterable<ToIntFunction<? super E>> hashFunctions) {
      this.bitsToUse = bitsToUse;
      this.intSet = intSet;
      this.hashFunctions = hashFunctions;
   }

   public static <E> BloomFilter<E> createFilter(int bitsToUse, Iterable<ToIntFunction<? super E>> hashFunctions) {
      return new BloomFilter<>(bitsToUse, IntSets.mutableEmptySet(bitsToUse), hashFunctions);
   }

   /**
    * Same as {@link #createFilter(int, Iterable)} except the returned {code BloomFilter} instance may be used
    * concurrently without additional synchronization.
    * <p>
    * The provided function iterable must be able to be iterated upon concurrently.
    * @param bitsToUse
    * @param hashFunctions
    * @param <E>
    * @return
    */
   public static <E> BloomFilter<E> createConcurrentFilter(int bitsToUse, Iterable<ToIntFunction<? super E>> hashFunctions) {
      return new BloomFilter<>(bitsToUse, IntSets.concurrentSet(bitsToUse), hashFunctions);
   }

   /**
    * Adds a value to the filter setting up to a number of bits equal to the number of hash functions. This method
    * will also return {code true} if any of the bits were updated, meaning this value was for sure not present before.
    * @param value the value to add to the filter
    * @return whether the filter was actually updated
    */
   public boolean addToFilter(E value) {
      boolean setABit = false;
      for (ToIntFunction<? super E> function : hashFunctions) {
         int hashResult = Math.abs(function.applyAsInt(value));
         int bitToCheck = hashResult % bitsToUse;
         setABit |= intSet.add(bitToCheck);
      }
      return setABit;
   }

   /**
    * Returns {@code true} if the element might be present, {@code false} if the value was for sure not present.
    * @param value the value to check for
    * @return whether this value may be present or for sure not
    */
   public boolean possiblyPresent(E value) {
      for (ToIntFunction<? super E> function : hashFunctions) {
         int hashResult = Math.abs(function.applyAsInt(value));
         int bitToCheck = hashResult % bitsToUse;
         if (!intSet.contains(bitToCheck)) {
            return false;
         }
      }
      return true;
   }

   /**
    * Clears all current bits and sets them to the values in the provided {@link IntSet}. Since this method clears
    * and sets the values any other concurrent operations may be ignored.
    * @param intSet
    */
   public void setBits(IntSet intSet) {
      this.intSet.clear();
      this.intSet.addAll(intSet);
   }

   public IntSet getIntSet() {
      return IntSets.immutableSet(intSet);
   }

   @Override
   public String toString() {
      return "BloomFilter{" +
            "bitsToUse=" + bitsToUse +
            ", intSet=" + intSet +
            '}';
   }
}
