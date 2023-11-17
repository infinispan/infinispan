package org.infinispan.commons.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

/**
 * Read-only representation of all the integers between an open interval.
 * <p>
 * This is a more general version of {@link RangeSet}.
 * </p>
 *
 * @author José Bolina
 * @since 15.0
 * @see RangeSet
 */
class OffsetIntSet extends AbstractImmutableIntSet {

   private final int start;
   private final int end;

   OffsetIntSet(int start, int exclusiveEnd) {
      assert exclusiveEnd >= start : "End should be greater or equal than start";

      this.start = start;
      this.end = exclusiveEnd;
   }


   @Override
   public boolean contains(int i) {
      return i >= start && i < end;
   }

   @Override
   public boolean containsAll(IntSet set) {
      if (set instanceof OffsetIntSet) {
         OffsetIntSet other = (OffsetIntSet) set;
         return other.start >= start && other.end <= end;
      }
      PrimitiveIterator.OfInt it = set.iterator();
      while (it.hasNext()) {
         if (!contains(it.nextInt()))
            return false;
      }

      return true;
   }

   @Override
   public int size() {
      return end - start;
   }

   @Override
   public boolean isEmpty() {
      return start == end;
   }

   @Override
   public boolean contains(Object o) {
      return o instanceof Integer && contains((int) o);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return intStream().iterator();
   }

   @Override
   public Object[] toArray() {
      int size = size();
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
         array[i] = start + i;
      }
      return array;
   }

   @Override
   public <T> T[] toArray(T[] a) {
      int size = size();
      T[] array = a.length >= size ? a :
            (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
      for (int i = 0; i < size; i++) {
         array[i] = (T) Integer.valueOf(i);
      }
      return array;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet)
         return containsAll((IntSet) c);

      for (Object o : c) {
         if (!contains(o))
            return false;
      }
      return true;
   }

   @Override
   public IntStream intStream() {
      return IntStream.range(start, end);
   }

   @Override
   public byte[] toBitSet() {
      int size = size();
      if (size == 0) {
         return Util.EMPTY_BYTE_ARRAY;
      }

      BitSet bs = new BitSet();
      bs.flip(start, end);
      return bs.toByteArray();
   }

   @Override
   public int nextSetBit(int fromIndex) {
      if (fromIndex <= start) return start;
      if (fromIndex >= end) return -1;

      return fromIndex;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof Collection)) return false;

      Collection<?> other = (Collection<?>) obj;
      return size() == other.size() && containsAll(other);
   }

   @Override
   public int hashCode() {
      return Objects.hash(start, end);
   }

   @Override
   public String toString() {
      return "{" + start + "-" + end + "}";
   }
}
