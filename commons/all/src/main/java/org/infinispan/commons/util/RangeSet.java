package org.infinispan.commons.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Read-only set representing all the integers from {@code 0} to {@code size - 1} (inclusive).
 *
 * @author Dan Berindei
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.INTSET_RANGE)
public class RangeSet implements IntSet {

   @ProtoField(number = 1, defaultValue = "0")
   final int size;

   @ProtoFactory
   public RangeSet(int size) {
      this.size = size;
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size <= 0;
   }

   @Override
   public boolean contains(Object o) {
      if (!(o instanceof Integer))
         return false;
      int i = (int) o;
      return contains(i);
   }

   @Override
   public boolean contains(int i) {
      return 0 <= i && i < size;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new RangeSetIterator(size);
   }

   @Override
   public int[] toIntArray() {
      int[] array = new int[size];
      for (int i = 0; i < size; i++) {
         array[i] = i;
      }
      return array;
   }

   @Override
   public byte[] toBitSet() {
      if (size == 0) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      int offset = (size >>> 3);
      if ((size & 0xf) == 0) {
         byte[] array = new byte[offset];
         Arrays.fill(array, (byte) 0xff);
         return array;
      }
      byte[] array = new byte[offset + 1];
      if (offset > 0) {
         Arrays.fill(array, 0, offset, (byte) 0xff);
      }
      int lastBitOffset = size > 8 ? size % 8 : size;
      array[offset] = (byte) (0xff >> (8 - lastBitOffset));
      return array;
   }

   @Override
   public int nextSetBit(int fromIndex) {
      return contains(fromIndex) ? fromIndex : -1;
   }

   @Override
   public Object[] toArray() {
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
         array[i] = i;
      }
      return array;
   }

   @Override
   public <T> T[] toArray(T[] a) {
      T[] array = a.length >= size ? a :
                  (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
      for (int i = 0; i < size; i++) {
         array[i] = (T) Integer.valueOf(i);
      }
      return array;
   }

   @Override
   public boolean add(Integer integer) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean remove(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return containsAll((IntSet) c);
      }
      for (Object o : c) {
         if (!contains(o))
            return false;
      }
      return true;
   }

   @Override
   public boolean containsAll(IntSet set) {
      if (set instanceof RangeSet) {
         return size >= ((RangeSet) set).size;
      }
      PrimitiveIterator.OfInt iter = set.iterator();
      while (iter.hasNext()) {
         if (!contains(iter.nextInt())) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean add(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   public void set(int i) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean addAll(IntSet set) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean retainAll(IntSet c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean removeAll(IntSet set) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || !(o instanceof Set))
         return false;

      if (o instanceof RangeSet) {
         RangeSet integers = (RangeSet) o;

         return size == integers.size;
      } else {
         Set set = (Set) o;
         return size == set.size() && containsAll(set);
      }
   }

   @Override
   public IntStream intStream() {
      return IntStream.range(0, size);
   }

   @Override
   public void forEach(IntConsumer action) {
      for (int i = 0; i < size; ++i) {
         action.accept(i);
      }
   }

   @Override
   public void forEach(Consumer<? super Integer> action) {
      for (int i = 0; i < size; ++i) {
         // Has cost of auto boxing, oh well
         action.accept(i);
      }
   }

   @Override
   public Spliterator.OfInt intSpliterator() {
      return new RangeSetSpliterator(size);
   }

   @Override
   public boolean removeIf(IntPredicate filter) {
      throw new UnsupportedOperationException("RangeSet is immutable");
   }

   @Override
   public int hashCode() {
      return size;
   }

   @Override
   public String toString() {
      return "RangeSet(" + size + ")";
   }

   private static class RangeSetSpliterator implements Spliterator.OfInt {
      private int next;
      private final int size;

      public RangeSetSpliterator(int size) {
         this.next = 0;
         this.size = size;
      }

      RangeSetSpliterator(int next, int size) {
         this.next = next;
         this.size = size;
      }

      @Override
      public OfInt trySplit() {
         int lo = next, mid = (lo + size) >>> 1;
         return (lo >= mid)
               ? null
               : new RangeSetSpliterator(lo, next = mid);
      }

      @Override
      public void forEachRemaining(IntConsumer action) {
         for (; next < size; ++next) {
            action.accept(next);
         }
      }

      @Override
      public long estimateSize() {
         return size - next;
      }

      @Override
      public int characteristics() {
         return SIZED | SUBSIZED | DISTINCT | SORTED | ORDERED | NONNULL | IMMUTABLE;
      }

      @Override
      public boolean tryAdvance(IntConsumer action) {
         if (next < size) {
            action.accept(next++);
            return true;
         }
         return false;
      }
   }

   private static class RangeSetIterator implements PrimitiveIterator.OfInt {
      private final int size;
      private int next;

      public RangeSetIterator(int size) {
         this.size = size;
         this.next = 0;
      }

      @Override
      public boolean hasNext() {
         return next < size;
      }

      @Override
      public int nextInt() {
         if (next >= size) {
            throw new NoSuchElementException();
         }
         return next++;
      }

      @Override
      public Integer next() {
         return nextInt();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("RangeSet is read-only");
      }
   }
}
