package org.infinispan.commons.util;

import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.IntConsumer;

/**
 * Static utility class for creating various {@link IntSet} objects.
 * @author wburns
 * @since 9.3
 */
public class IntSets {
   private IntSets() { }

   /**
    * Returns an immutable IntSet containing no values
    * @return IntSet with no values
    */
   public static IntSet immutableEmptySet() {
      return EmptyIntSet.getInstance();
   }

   /**
    * Returns an immutable IntSet containing a single value
    * @param value the value to be set on the IntSet
    * @return IntSet with just the 1 value
    */
   public static IntSet immutableSet(int value) {
      return new SingletonIntSet(value);
   }

   /**
    * Returns an immutable IntSet that wraps the given IntSet to prevent modifications.
    * @param set set to wrap
    * @return immutable IntSet
    */
   public static IntSet immutableSet(IntSet set) {
      if (set instanceof AbstractImmutableIntSet)
         return set;

      return new ImmutableIntSet(set);
   }

   /**
    * Returns an immutable IntSet containing all values from {@code 0} to {@code endExclusive - 1}.
    * @param endExclusive the exclusive upper bound
    * @return IntSet with the values in the given range available
    */
   public static IntSet immutableRangeSet(int endExclusive) {
      return new RangeSet(endExclusive);
   }

   /**
    * Returns an IntSet based on the provided Set. This method tries to return or create the most performant IntSet
    * based on the Set provided. If the Set is already an IntSet it will just return that. The returned IntSet may or
    * may not be immutable, so no guarantees are provided from that respect.
    * @param integerSet IntSet to create from
    * @return the IntSet that is equivalent to the Set
    */
   public static IntSet from(Set<Integer> integerSet) {
      if (integerSet instanceof IntSet) {
         return (IntSet) integerSet;
      }
      int size = integerSet.size();
      switch (size) {
         case 0:
            return EmptyIntSet.getInstance();
         case 1:
            return new SingletonIntSet(integerSet.iterator().next());
         default:
            return SmallIntSet.from(integerSet);
      }
   }

   public static IntSet from(byte[] bytes) {
      int size = bytes.length;
      if (size == 0) {
         return EmptyIntSet.getInstance();
      }
      return SmallIntSet.from(bytes);
   }

   /**
    * Returns an IntSet based on the ints in the iterator. This method will try to return the most performant IntSet
    * based on what ints are provided if any. The returned IntSet may or may not be immutable, so no guarantees are
    * provided from that respect.
    * @param iterator values set in the returned set
    * @return IntSet with all the values set that the iterator had
    */
   public static IntSet from(PrimitiveIterator.OfInt iterator) {
      boolean hasNext = iterator.hasNext();
      if (!hasNext) {
         return EmptyIntSet.getInstance();
      }
      int firstValue = iterator.nextInt();
      hasNext = iterator.hasNext();
      if (!hasNext) {
         return new SingletonIntSet(firstValue);
      }
      // We have 2 or more values so just set them in the SmallIntSet
      SmallIntSet set = new SmallIntSet();
      set.set(firstValue);
      iterator.forEachRemaining((IntConsumer) set::set);
      return set;
   }

   /**
    * Returns an IntSet that is mutable that contains all of the values from the given set. If this provided Set is
    * already an IntSet and mutable it will return the same object.
    * @param integerSet set to use values from
    * @return IntSet that is mutable with the values set
    */
   public static IntSet mutableFrom(Set<Integer> integerSet) {
      if (integerSet instanceof SmallIntSet) {
         return (SmallIntSet) integerSet;
      }
      if (integerSet instanceof ConcurrentSmallIntSet) {
         return (ConcurrentSmallIntSet) integerSet;
      }
      return mutableCopyFrom(integerSet);
   }

   /**
    * Returns an IntSet that contains all ints from the given Set that is mutable. Updates to the original Set or
    * the returned IntSet are not reflected in the other.
    * @param mutableSet set to copy from
    * @return IntSet with the values set
    */
   public static IntSet mutableCopyFrom(Set<Integer> mutableSet) {
      if (mutableSet instanceof SingletonIntSet) {
         return mutableSet(((SingletonIntSet) mutableSet).value);
      }
      return new SmallIntSet(mutableSet);
   }

   /**
    * Returns an IntSet that contains no values but is initialized to hold ints equal to the {@code maxExclusive -1} or
    * smaller.
    * @param maxExclusive largest int expected in set
    * @return IntSet with no values set
    */
   public static IntSet mutableEmptySet(int maxExclusive) {
      return new SmallIntSet(maxExclusive);
   }

   /**
    * Returns a mutable IntSet with no values set. Note this mutable set is initialized given a default size. If you wish
    * to not have less initialization over, please use {@link #mutableEmptySet(int)} providing a {@code 0} or similar.
    * @return IntSet with no values set
    */
   public static IntSet mutableEmptySet() {
      return new SmallIntSet();
   }

   /**
    * Returns a mutable set with the initial value set. This set is optimized to insert values less than this. Values
    * added that are larger may require additional operations.
    * @param value the value to set
    * @return IntSet with the value set
    */
   public static IntSet mutableSet(int value) {
      return SmallIntSet.of(value);
   }

   /**
    * Returns a mutable IntSet that begins with the initialized values
    * @param value1
    * @param value2
    * @return
    */
   public static IntSet mutableSet(int value1, int value2) {
      return SmallIntSet.of(value1, value2);
   }

   /**
    * Returns a concurrent mutable IntSet that can store values in the range of {@code 0..maxExclusive-1}
    * @param maxExclusive the maximum value - 1 that can be inserted into the set
    * @return concurrent set
    */
   public static IntSet concurrentSet(int maxExclusive) {
      // if maxExclusive = 0; then we have an empty set
      return maxExclusive == 0 ? immutableEmptySet() : new ConcurrentSmallIntSet(maxExclusive);
   }

   /**
    * Returns a copy of the given set that supports concurrent operations. The returned set will contain all of the
    * ints the provided set contained. The returned mutable IntSet can store values in the range of {@code 0..maxExclusive-1}
    * @param intSet set to copy from
    * @param maxExclusive the maximum value - 1 that can be inserted into the set
    * @return concurrent copy
    */
   public static IntSet concurrentCopyFrom(IntSet intSet, int maxExclusive) {
      // if maxExclusive = 0; then we have an empty set
      if (maxExclusive == 0) {
         return immutableEmptySet();
      }
      ConcurrentSmallIntSet cis = new ConcurrentSmallIntSet(maxExclusive);
      intSet.forEach((IntConsumer) cis::set);
      return cis;
   }
}
