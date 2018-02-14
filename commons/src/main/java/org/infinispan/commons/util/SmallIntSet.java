package org.infinispan.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.commons.io.UnsignedNumeric;

/**
 * Represent a set of integers (e.g. segments) as a {@code BitSet}.
 *
 * Memory usage depends on the highest element, as in {@link BitSet} and unlike in other collections such as
 * {@link java.util.HashSet}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class SmallIntSet implements IntSet {
   private final BitSet bitSet;

   public static SmallIntSet of(int i1) {
      SmallIntSet set = new SmallIntSet();
      set.set(i1);
      return set;
   }

   public static SmallIntSet of(int i1, int i2) {
      SmallIntSet set = new SmallIntSet();
      set.set(i1);
      set.set(i2);
      return set;
   }

   public static SmallIntSet of(int i1, int i2, int i3) {
      SmallIntSet set = new SmallIntSet();
      set.set(i1);
      set.set(i2);
      set.set(i3);
      return set;
   }

   public static SmallIntSet of(int... elements) {
      SmallIntSet set = new SmallIntSet();
      for (int i : elements) {
         set.set(i);
      }
      return set;
   }

   public static SmallIntSet of(PrimitiveIterator.OfInt iterator) {
      SmallIntSet set = new SmallIntSet();
      iterator.forEachRemaining((IntConsumer) set::set);
      return set;
   }

   /**
    * Either converts the given set to an IntSet if it is one or creates a new IntSet and copies the contents
    * @param set
    * @return
    */
   public static SmallIntSet from(Set<Integer> set) {
      if (set instanceof SmallIntSet) {
         return (SmallIntSet) set;
      } else {
         return new SmallIntSet(set);
      }
   }

   public SmallIntSet() {
      bitSet = new BitSet();
   }

   /**
    * Create a new {@code IntSet} and pre-allocate space for elements {@code 0..initialRange-1}.
    */
   public SmallIntSet(int initialRange) {
      bitSet = new BitSet(initialRange);
   }

   public SmallIntSet(SmallIntSet other) {
      bitSet = new BitSet(other.bitSet.size());
      bitSet.or(other.bitSet);
   }

   public SmallIntSet(Set<Integer> set) {
      bitSet = new BitSet();
      set.forEach(bitSet::set);
   }

   public SmallIntSet(IntSet set) {
      if (set instanceof SmallIntSet) {
         BitSet bitSet = ((SmallIntSet) set).bitSet;
         this.bitSet = new BitSet(bitSet.size());
         this.bitSet.or(bitSet);
      } else {
         this.bitSet = new BitSet();
         set.iterator().forEachRemaining((IntConsumer) bitSet::set);
      }
   }

   @Override
   public int size() {
      return bitSet.cardinality();
   }

   public int capacity() {
      return bitSet.size();
   }

   @Override
   public boolean isEmpty() {
      return bitSet.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return (o instanceof Integer) && contains((Integer) o);
   }

   public boolean contains(Integer o) {
      return bitSet.get(o);
   }

   /**
    * Check if the set contains an integer without boxing the parameter.
    */
   public boolean contains(int i) {
      return bitSet.get(i);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new BitSetIntIterator(bitSet);
   }

   static class BitSetIntIterator implements PrimitiveIterator.OfInt {
      private final BitSet bitSet;
      private int offset;
      private int prev;

      BitSetIntIterator(BitSet bitSet) {
         this.bitSet = bitSet;
         this.offset = bitSet.nextSetBit(0);
         this.prev = -1;
      }

      @Override
      public int nextInt() {
         if (offset < 0) {
            throw new NoSuchElementException();
         }
         prev = offset;
         offset = bitSet.nextSetBit(offset + 1);
         return prev;
      }

      @Override
      public boolean hasNext() {
         return offset >= 0;
      }

      @Override
      public void remove() {
         if (prev < 0) {
            throw new IllegalStateException();
         }
         bitSet.clear(prev);
         prev = -1;
      }
   }

   @Override
   public Object[] toArray() {
      int size = size();
      Integer[] dest = new Integer[size];
      copyToArray(size, dest);
      return dest;
   }

   @Override
   public <T> T[] toArray(T[] a) {
      if (!(a instanceof Integer[])) {
         throw new IllegalArgumentException("Only Integer arrays are supported");
      }
      int size = size();
      Integer[] dest = a.length < size ? new Integer[size] : (Integer[]) a;
      copyToArray(size, dest);
      return (T[]) dest;
   }

   private void copyToArray(int size, Integer[] dest) {
      int lastSetBit = -1;
      for (int i = 0; i < size; i++) {
         lastSetBit = bitSet.nextSetBit(lastSetBit + 1);
         dest[i] = lastSetBit;
      }
   }

   @Override
   public boolean add(Integer i) {
      return add((int) i);
   }

   /**
    * Add an integer to the set without boxing the parameter.
    */
   public boolean add(int i) {
      boolean wasSet = bitSet.get(i);
      if (!wasSet) {
         bitSet.set(i);
         return true;
      }
      return false;
   }

   /**
    * Add an integer to the set without boxing the parameter or checking if the integer was already present in the set.
    */
   public void set(int i) {
      bitSet.set(i);
   }

   /**
    * If {@code value} is {@code true}, add the integer to the set, otherwise remove the integer from the set.
    */
   public void set(int i, boolean value) {
      bitSet.set(i, value);
   }

   @Override
   public boolean remove(Object o) {
      if (!(o instanceof Integer)) {
         return false;
      }
      return remove((int) o);
   }

   /**
    * Remove an integer from the set without boxing.
    */
   public boolean remove(int i) {
      boolean wasSet = bitSet.get(i);
      if (wasSet) {
         bitSet.clear(i);
         return true;
      }
      return false;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return containsAll((IntSet) c);
      }
      return c.stream().allMatch(this::contains);
   }

   @Override
   public boolean containsAll(IntSet set) {
      if (set instanceof SmallIntSet) {
         BitSet bs = ((SmallIntSet) set).bitSet;
         for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
            if (!bitSet.get(i)) {
               return false;
            }
         }
         return true;
      }
      PrimitiveIterator.OfInt iter = set.iterator();
      while (iter.hasNext()) {
         if (!bitSet.get(iter.nextInt())) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean addAll(IntSet set) {
      boolean modified = false;
      if (set instanceof SmallIntSet) {
         int countBefore = bitSet.cardinality();
         bitSet.or(((SmallIntSet) set).bitSet);
         modified = countBefore != bitSet.cardinality();

      } else {
         PrimitiveIterator.OfInt iter = set.iterator();
         while (iter.hasNext()) {
            modified |= add(iter.nextInt());
         }
      }

      return modified;
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      boolean modified = false;
      if (c instanceof IntSet) {
         return addAll((IntSet) c);
      } else {
         for (Integer integer : c) {
            modified |= add(integer);
         }
      }
      return modified;
   }

   @Override
   public boolean removeAll(IntSet set) {
      boolean modified = false;
      if (set instanceof SmallIntSet) {
         int countBefore = bitSet.cardinality();
         bitSet.andNot(((SmallIntSet) set).bitSet);
         modified = countBefore != bitSet.cardinality();

      } else {
         PrimitiveIterator.OfInt iter = set.iterator();
         while (iter.hasNext()) {
            modified |= remove(iter.nextInt());
         }
      }

      return modified;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return removeAll((IntSet) c);
      }
      boolean modified = false;
      for (Object integer : c) {
         modified |= remove(integer);
      }
      return modified;
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return retainAll((IntSet) c);
      }
      boolean modified = false;
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
         if (!c.contains(i)) {
            bitSet.clear(i);
            modified = true;
         }
      }
      return modified;
   }

   @Override
   public boolean retainAll(IntSet c) {
      boolean modified = false;
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
         if (!c.contains(i)) {
            bitSet.clear(i);
            modified = true;
         }
      }
      return modified;
   }

   @Override
   public void clear() {
      bitSet.clear();
   }

   @Override
   public IntStream intStream() {
      return bitSet.stream();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || !(o instanceof Set))
         return false;

      if (o instanceof SmallIntSet) {
         SmallIntSet integers = (SmallIntSet) o;
         return bitSet.equals(integers.bitSet);
      } else {
         Set set = (Set) o;
         return size() == set.size() && containsAll(set);
      }
   }

   @Override
   public int hashCode() {
      return bitSet.hashCode();
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("{");
      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
         if (sb.length() > "{".length()) {
            sb.append(' ');
         }
         int runStart = i;
         while (bitSet.get(i + 1)) {
            i++;
         }
         if (i == runStart) {
            sb.append(i);
         } else {
            sb.append(runStart).append('-').append(i);
         }
      }
      sb.append('}');
      return sb.toString();
   }

   public static void writeTo(ObjectOutput output, SmallIntSet set) throws IOException {
      UnsignedNumeric.writeUnsignedInt(output, set.capacity());
      UnsignedNumeric.writeUnsignedInt(output, set.size());
      for (int element : set) {
         UnsignedNumeric.writeUnsignedInt(output, element);
      }
   }

   public static SmallIntSet readFrom(ObjectInput input) throws IOException {
      int capacity = UnsignedNumeric.readUnsignedInt(input);
      int size = UnsignedNumeric.readUnsignedInt(input);

      SmallIntSet set = new SmallIntSet(capacity);
      for (int i = 0; i < size; i++) {
         int element = UnsignedNumeric.readUnsignedInt(input);
         set.set(element);
      }
      return set;
   }
}
