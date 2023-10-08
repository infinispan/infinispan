package org.infinispan.commons.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Concurrent implementation of an {@link IntSet}. This implementation is limited in that it can only inserts ints up
 * to an initialized maximum. Any attempt to insert/remove a larger value will result in an
 * {@link IllegalArgumentException} thrown. Note that operations spanning multiple values (ie.
 * {@link #containsAll(IntSet)}, {@link #removeAll(IntSet)}) are not performed atomically and are done on a per value
 * basis.
 * @author wburns
 * @since 9.3
 */
@ProtoTypeId(ProtoStreamTypeIds.INTSET_CONCURRENT_SMALL)
public class ConcurrentSmallIntSet implements IntSet {

   final AtomicIntegerArray array;

   // Note per Java Language Specification 15.19 Shift Operators
   // If the promoted type of the left-hand operand is int, only the five lowest-order bits of the right-hand operand
   // are used as the shift distance.
   private static final int ADDRESS_BITS_PER_INT = 5;

   /* Used to shift left or right for a partial int mask */
   private static final int INT_MASK = 0xffff_ffff;

   private final AtomicInteger currentSize = new AtomicInteger();

   /**
    * Creates a new, empty map which can accommodate ints in value up to {@code maxCapacityExclusive - 1}. This number
    * will be rounded up to the nearest 32.
    * @param maxCapacityExclusive The implementation performs sizing to ensure values up to this can be stored
    */
   public ConcurrentSmallIntSet(int maxCapacityExclusive) {
      if (maxCapacityExclusive < 1) {
         throw new IllegalArgumentException("maxCapacityExclusive (" + maxCapacityExclusive + ") < 1");
      }
      // We add 31 as that is 2^5 -1 so we round up
      int intLength = intIndex(maxCapacityExclusive + 31);
      array = new AtomicIntegerArray(intLength);
   }

   @ProtoFactory
   static ConcurrentSmallIntSet protoFactory(List<Integer> entries) {
      int arrayLength = entries.size();
      ConcurrentSmallIntSet intSet = new ConcurrentSmallIntSet(arrayLength << ADDRESS_BITS_PER_INT);

      int size = 0;
      for (int i = 0; i < arrayLength - 1; ++i) {
         int value = entries.get(i);
         // Use lazy set - we use set below on the last
         intSet.array.lazySet(i, value);
         size += Integer.bitCount(value);
      }
      int lastValue = entries.get(arrayLength - 1);
      intSet.array.set(arrayLength - 1, lastValue);
      size += Integer.bitCount(lastValue);
      intSet.currentSize.addAndGet(size);

      return intSet;
   }

   @ProtoField(1)
   Iterable<Integer> entries() {
      return () -> new Iterator<>() {
         int i = 0;
         @Override
         public boolean hasNext() {
            return i < array.length();
         }

         @Override
         public Integer next() {
            return array.get(i++);
         }
      };
   }

   private void valueNonZero(int value) {
      if (value < 0) {
         throw new IllegalArgumentException("The provided value " + value + " must be 0 or greater");
      }
   }

   private void checkBounds(int index) {
      if (index >= array.length()) {
         throw new IllegalArgumentException("Provided integer " + index + " was larger than originally initialized size " + array.length());
      }
   }

   private int intIndex(int bitIndex) {
      return bitIndex >> ADDRESS_BITS_PER_INT;
   }

   // Same idea as BitSet#nextSetBit
   @Override
   public int nextSetBit(int fromIndex) {
      if (fromIndex < 0)
         throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

      int u = intIndex(fromIndex);
      int arrayLength = array.length();
      if (u >= arrayLength)
         return -1;

      int possible = array.get(u) & (INT_MASK << fromIndex);

      while (true) {
         if (possible != 0) {
            return (u << ADDRESS_BITS_PER_INT) + Integer.numberOfTrailingZeros(possible);
         }
         if (++u == arrayLength) {
            return -1;
         }
         possible = array.get(u);
      }
   }

   @Override
   public boolean add(int i) {
      valueNonZero(i);
      int bit = 1 << i;
      int idx = intIndex(i);
      checkBounds(idx);
      while (true) {
         int num = array.get(idx);
         int num2 = num | bit;
         if (num == num2) {
            return false;
         }
         if (array.compareAndSet(idx, num, num2)) {
            currentSize.incrementAndGet();
            return true;
         }
      }
   }

   @Override
   public void set(int i) {
      // No real optimizations for this so we just invoke add
      add(i);
   }

   @Override
   public boolean remove(int i) {
      valueNonZero(i);
      int idx = intIndex(i);
      checkBounds(idx);
      int bit = 1 << i;
      while (true) {
         int num = array.get(idx);

         int unsetNum = num & ~bit;
         if (num == unsetNum) {
            return false;
         }
         if (array.compareAndSet(idx, num, unsetNum)) {
            currentSize.decrementAndGet();
            return true;
         }
      }
   }

   @Override
   public boolean contains(int i) {
      valueNonZero(i);
      int idx = intIndex(i);
      if (idx >= array.length()) {
         return false;
      }
      int num = array.get(idx);

      int bit = 1 << i;
      return (num & bit) != 0;
   }

   @Override
   public boolean addAll(IntSet set) {
      boolean changed = false;
      for (PrimitiveIterator.OfInt iter = set.iterator(); iter.hasNext(); ) {
         changed |= add(iter.nextInt());
      }
      return changed;
   }

   @Override
   public boolean containsAll(IntSet set) {
      for (PrimitiveIterator.OfInt iter = set.iterator(); iter.hasNext(); ) {
         if (!contains(iter.nextInt())) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean removeAll(IntSet set) {
      boolean modified = false;
      for (PrimitiveIterator.OfInt iter = set.iterator(); iter.hasNext(); ) {
         modified |= remove(iter.nextInt());
      }
      return modified;
   }

   @Override
   public boolean retainAll(IntSet set) {
      boolean modified = false;
      for (int i = 0; i < array.length(); ++i) {
         int posValue = array.get(i);
         int offset = 1;
         // We iterate through the current value by always checking the least significant bit and shifting right
         // until the number finally reaches zero
         while (posValue > 0) {
            if ((posValue & 1) == 1) {
               int ourValue = (i << ADDRESS_BITS_PER_INT) + offset - 1;
               if (!set.contains(ourValue)) {
                  modified |= remove(ourValue);
               }
            }
            posValue >>= 1;
            offset += 1;
         }
      }
      return modified;
   }

   @Override
   public int size() {
      return currentSize.get();
   }

   @Override
   public boolean isEmpty() {
      return currentSize.get() == 0;
   }

   @Override
   public boolean contains(Object o) {
      return (o instanceof Integer) && contains((int) o);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new ConcurrentIntIterator();
   }

   private class ConcurrentIntIterator implements PrimitiveIterator.OfInt {
      private int currentValue;
      private int prevValue = -1;

      ConcurrentIntIterator() {
         currentValue = nextSetBit(0);
      }

      @Override
      public int nextInt() {
         if (currentValue < 0) {
            throw new NoSuchElementException();
         }
         prevValue = currentValue;
         currentValue = nextSetBit(currentValue + 1);
         return prevValue;
      }

      @Override
      public boolean hasNext() {
         return currentValue >= 0;
      }

      @Override
      public void remove() {
         if (prevValue < 0) {
            throw new IllegalStateException();
         }
         ConcurrentSmallIntSet.this.remove(prevValue);
         prevValue = -1;
      }
   }

   @Override
   public final Object[] toArray() {
      int size = currentSize.get();
      Object[] r = new Object[size];
      int index = 0;

      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         int offset = 1;
         while (value > 0) {
            if ((value & 1) == 1) {
               if (index == size) {
                  size += (size >>> 1) + 1;
                  r = Arrays.copyOf(r, size);
               }
               r[index++] = (i << ADDRESS_BITS_PER_INT) + offset - 1;
            }
            value >>= 1;
            offset += 1;
         }
      }

      return (index == size) ? r : Arrays.copyOf(r, size);
   }

   @SuppressWarnings("unchecked")
   @Override
   public final <T> T[] toArray(T[] a) {
      int currentSize = this.currentSize.get();
      T[] r = (a.length >= currentSize) ? a :
            (T[])java.lang.reflect.Array
                  .newInstance(a.getClass().getComponentType(), currentSize);
      int n = r.length;
      int i = 0;
      for (Integer e : this) {
         if (i == n) {
            n += (n >>> 1) + 1;
            r = Arrays.copyOf(r, n);
         }
         r[i++] = (T) e;
      }
      if (a == r && i < n) {
         r[i] = null; // null-terminate
         return r;
      }
      return (i == n) ? r : Arrays.copyOf(r, i);
   }

   @Override
   public boolean add(Integer integer) {
      return add((int) integer);
   }

   @Override
   public boolean remove(Object o) {
      return (o instanceof Integer) && remove((int) o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (c instanceof IntSet) {
         return containsAll((IntSet) c);
      }
      for (Object obj: c) {
         if (!contains(obj)) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      if (c instanceof IntSet) {
         return addAll((IntSet) c);
      }
      boolean changed = false;
      for (Integer integer : c) {
         changed |= add(integer);
      }
      return changed;
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      boolean modified = false;
      for (PrimitiveIterator.OfInt iter = iterator(); iter.hasNext(); ) {
         int value = iter.nextInt();
         if (!c.contains(value)) {
            iter.remove();
            modified = true;
         }
      }
      return modified;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      boolean modified = false;
      for (Object value : c) {
         modified |= remove(value);
      }
      return modified;
   }

   @Override
   public void clear() {
      for (int i = 0; i < array.length(); ++i) {
         int oldValue = array.getAndSet(i, 0);
         int bitsSet = Integer.bitCount(oldValue);
         if (bitsSet != 0) {
            currentSize.addAndGet(-bitsSet);
         }
      }
   }

   @Override
   public IntStream intStream() {
      return StreamSupport.intStream(intSpliterator(), false);
   }

   @Override
   public Spliterator.OfInt intSpliterator() {
      // We just invoke default method as ints can be sparse in AtomicReferenceArray
      return IntSet.super.intSpliterator();
   }

   @Override
   public int[] toIntArray() {
      int size = currentSize.get();
      int[] r = new int[size];
      int index = 0;

      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         int offset = 1;
         while (value != 0) {
            if ((value & 1) == 1) {
               if (index == size) {
                  size += (size >>> 1) + 1;
                  r = Arrays.copyOf(r, size);
               }
               r[index++] = (i << ADDRESS_BITS_PER_INT) + offset - 1;
            }
            value >>>= 1;
            offset += 1;
         }
      }

      return (index == size) ? r : Arrays.copyOf(r, size);
   }

   @Override
   public byte[] toBitSet() {
      byte[] bytes = new byte[array.length() * 8];
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         bb.putInt(value);

      }
      return bb.array();
   }

   @Override
   public void forEach(Consumer<? super Integer> action) {
      if (action instanceof IntConsumer) {
         forEach((IntConsumer) action);
      } else {
         forEach((IntConsumer) action::accept);
      }
   }

   @Override
   public void forEach(IntConsumer action) {
      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         int offset = 1;
         while (value != 0) {
            if ((value & 1) == 1) {
               action.accept((i << ADDRESS_BITS_PER_INT) + offset - 1);
            }
            value >>>= 1;
            offset += 1;
         }
      }
   }

   @Override
   public boolean removeIf(Predicate<? super Integer> filter) {
      if (filter instanceof IntPredicate) {
         return removeIf((IntPredicate) filter);
      } else {
         return removeIf((IntPredicate) filter::test);
      }
   }

   @Override
   public boolean removeIf(IntPredicate filter) {
      boolean modified = false;
      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         int offset = 1;
         while (value != 0) {
            if ((value & 1) == 1) {
               int ourValue = (i << ADDRESS_BITS_PER_INT) + offset - 1;
               if (filter.test(ourValue)) {
                  modified |= remove(ourValue);
               }
            }
            value >>>= 1;
            offset += 1;
         }
      }
      return modified;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || !(o instanceof Set))
         return false;

      Set set = (Set) o;
      // containsAll handles casting as necessary
      return size() == set.size() && containsAll(set);
   }

   @Override
   public int hashCode() {
      int hashCode = 0;
      for (int i = 0; i < array.length(); ++i) {
         int value = array.get(i);
         hashCode *= 37;
         hashCode += value;
      }
      return hashCode;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("{");
      for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
         if (sb.length() > "{".length()) {
            sb.append(' ');
         }
         int runStart = i;
         while (contains(i + 1)) {
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
}
