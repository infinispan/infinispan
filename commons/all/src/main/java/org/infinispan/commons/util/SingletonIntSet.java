package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Immutable implementation of IntSet that contains a single value
 * @author wburns
 * @since 9.3
 */
@ProtoTypeId(ProtoStreamTypeIds.INTSET_SINGLETON)
public class SingletonIntSet extends AbstractImmutableIntSet {

   @ProtoField(1)
   final int value;

   @ProtoFactory
   SingletonIntSet(int value) {
      if (value < 0) {
         throw new IllegalArgumentException("Value must be 0 or greater");
      }
      this.value = value;
   }

   @Override
   public boolean contains(int i) {
      return value == i;
   }

   @Override
   public boolean containsAll(IntSet set) {
      int size = set.size();
      return size == 0 || size == 1 && set.contains(value);
   }

   @Override
   public int size() {
      return 1;
   }

   @Override
   public boolean isEmpty() {
      return false;
   }

   @Override
   public boolean contains(Object o) {
      return o instanceof Integer && ((Integer) o) == value;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new SingleIntIterator();
   }

   @Override
   public byte[] toBitSet() {
      int offset = (value >>> 3);
      byte[] array = new byte[offset + 1];
      int lastBitOffset = value > 8 ? value % 8 : value;
      // Need to use logical right shift to ensure other bits aren't set
      array[offset] = (byte) (0x80 >>> (7 - lastBitOffset));
      return array;
   }

   @Override
   public int nextSetBit(int fromIndex) {
      return contains(fromIndex) ? fromIndex : -1;
   }

   private class SingleIntIterator implements PrimitiveIterator.OfInt {
      boolean available = true;

      @Override
      public int nextInt() {
         if (!available) {
            throw new NoSuchElementException();
         }
         available = false;
         return value;
      }

      @Override
      public boolean hasNext() {
         return available;
      }
   }

   @Override
   public Object[] toArray() {
      Object[] array = new Object[1];
      array[0] = value;
      return array;
   }

   @Override
   public <T> T[] toArray(T[] a) {
      if (!(a instanceof Integer[])) {
         throw new IllegalArgumentException("Only Integer arrays are supported");
      }
      T[] r = (a.length >= 1) ? a :
            (T[])java.lang.reflect.Array
                  .newInstance(a.getClass().getComponentType(), 1);
      r[0] = (T) Integer.valueOf(value);
      if (r.length > 1) {
         r[1] = null;
      }
      return r;
   }

   @Override
   public int[] toIntArray() {
      int[] array = new int[1];
      array[0] = value;
      return array;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return containsAll((IntSet) c);
      }
      return c.size() == 1 && c.contains(value);
   }

   @Override
   public IntStream intStream() {
      return IntStream.of(value);
   }

   @Override
   public void forEach(IntConsumer action) {
      action.accept(value);
   }

   @Override
   public void forEach(Consumer<? super Integer> action) {
      if (action instanceof IntConsumer) {
         forEach((IntConsumer) action);
      }
      action.accept(value);
   }

   @Override
   public Spliterator.OfInt intSpliterator() {
      return new SingletonSpliterator();
   }

   @Override
   public Spliterator<Integer> spliterator() {
      return new SingletonSpliterator();
   }

   private class SingletonSpliterator implements Spliterator.OfInt {
      boolean consumed = false;

      @Override
      public OfInt trySplit() {
         return null;
      }

      @Override
      public long estimateSize() {
         return 1;
      }

      @Override
      public int characteristics() {
         return SIZED | NONNULL | IMMUTABLE | DISTINCT | ORDERED;
      }

      @Override
      public boolean tryAdvance(IntConsumer action) {
         if (!consumed) {
            consumed = true;
            action.accept(value);
            return true;
         }
         return false;
      }

      @Override
      public Comparator<? super Integer> getComparator() {
         return null;
      }
   }

   @Override
   public int hashCode() {
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (o == this) {
         return true;
      }

      if (o == null || !(o instanceof Set))
         return false;

      if (o instanceof IntSet) {
         IntSet intSet = (IntSet) o;
         return intSet.size() == 1 && intSet.contains(value);
      } else {
         Set set = (Set) o;
         return set.size() == 1 && set.contains(value);
      }
   }

   @Override
   public String toString() {
      return "{" + value + "}";
   }
}
