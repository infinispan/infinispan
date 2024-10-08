package org.infinispan.commons.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ObjLongConsumer;

/**
 * It's a growable ring buffer that allows to move tail/head sequences, clear, append, set/replace at specific positions.
 * This version requires headSequence to always be pointing at the first non null value if one is present.
 *
 * @author Francesco Nigro
 */
public final class ArrayRingBuffer<T> {

   private static final Object[] EMPTY = new Object[0];
   // it points to the next slot after the last element
   private long tailSequence;
   // it points to the slot of the first element
   private long headSequence;
   private T[] elements;

   public ArrayRingBuffer() {
      this(0, 0);
   }

   public ArrayRingBuffer(final int initialSize) {
      this(initialSize, 0);
   }

   public ArrayRingBuffer(final int initialSize, final long headSequence) {
      if (initialSize < 0) {
         throw new IllegalArgumentException("initialSize must be greater than 0");
      }
      if (headSequence < 0) {
         throw new IllegalArgumentException("headSequence cannot be negative");
      }
      this.elements = allocate(initialSize == 0 ? 0 : findNextPositivePowerOfTwo(initialSize));
      this.headSequence = headSequence;
      this.tailSequence = headSequence;
   }

   public int size() {
      return size(true);
   }

   public int size(boolean count_null_elements) {
      if(count_null_elements)
         return (int) (tailSequence - headSequence);
      int size=0;
      final T[] els=this.elements;
      for(long i=headSequence; i < tailSequence; i++) {
         final T e = els[bufferOffset(i)];
         if(e != null)
            size++;
      }
      return size;
   }

   public long getTailSequence() {
      return tailSequence;
   }

   public long getHeadSequence() {
      return headSequence;
   }

   public void forEach(ObjLongConsumer<? super T> consumer) {
      final int size = size();
      final T[] elements = this.elements;
      long sequence = headSequence;
      for (int i = 0; i < size; i++,sequence++) {
         final T e = elements[bufferOffset(sequence)];
         // sequence++;
         if (e == null) {
            continue;
         }
         consumer.accept(e, sequence);
      }
   }

   public int availableCapacityWithoutResizing() {
      return elements.length - size();
   }

   private void validateIndex(final long index) {
      validateIndex(index, true);
   }

   private void validateIndex(final long index, final boolean validateTail) {
      if (index < 0) {
         throw new IllegalArgumentException("index cannot be negative");
      }
      if (validateTail && index >= tailSequence) {
         throw new IllegalArgumentException("index cannot be greater then " + tailSequence);
      }
      if (index < headSequence) {
         throw new IllegalArgumentException("index cannot be less then " + headSequence);
      }
   }

   public boolean contains(final long index) {
      return index < tailSequence && index >= headSequence;
   }

   public void dropTailToHead() {
      dropTailTo(getHeadSequence());
   }

   public void clear() {
      dropHeadUntil(getTailSequence());
   }

   /**
    * Remove all elements from {@code tailSequence} back to {@code indexInclusive}.<br>
    * At the end of this operation {@code tailSequence} is equals to {@code indexInclusive}.
    * Note there can be null values between {@code tailSequence} and {@code headSequence} as only
    * {@code headSequence} is caught up to a non null value on modification.
    *
    * <pre>
    * eg:
    * elements = [A, B, C]
    * tail = 3
    * head = 0
    *
    * dropTailTo(1)
    *
    * elements = [A]
    * tail = 1
    * head = 0
    * </pre>
    */
   public int dropTailTo(final long indexInclusive) {
      if (size() == 0 || !contains(indexInclusive)) {
         return 0;
      }
      final int fromIncluded = bufferOffset(indexInclusive);
      final int toExcluded = bufferOffset(tailSequence);
      final T[] elements = this.elements;
      final int clearFrom;
      int removed = 0;
      if (toExcluded <= fromIncluded) {
         Arrays.fill(elements, fromIncluded, elements.length, null);
         removed += elements.length - fromIncluded;
         clearFrom = 0;
      } else {
         clearFrom = fromIncluded;
      }
      Arrays.fill(elements, clearFrom, toExcluded, null);
      removed += (toExcluded - clearFrom);
      tailSequence = indexInclusive;
      return removed;
   }

   /**
    * Remove all elements from {@code headSequence} to at least {@code indexExclusive} until a non
    * null value is found if present.<br>
    * At the end of this operation {@code headSequence} will be greater than or equal {@code indexExclusive}.
    * If there are no more values left then {@code headSequence} will equal {@code tailSequence}.
    *
    * <pre>
    * eg:
    * elements = [A, B, C]
    * tail = 3
    * head = 0
    *
    * dropHeadUntil(1)
    *
    * elements = [B, C]
    * tail = 3
    * head = 1
    * </pre>
    */
   public int dropHeadUntil(final long indexExclusive) {
      final long indexInclusive = indexExclusive - 1;
      if (size() == 0 || !contains(indexInclusive)) {
         return 0;
      }
      final int fromInclusive = bufferOffset(headSequence);
      final int toExclusive = bufferOffset(indexExclusive);
      final T[] elements = this.elements;
      final int clearFrom;
      int removed = 0;
      if (toExclusive <= fromInclusive) {
         Arrays.fill(elements, fromInclusive, elements.length, null);
         removed += (elements.length - fromInclusive);
         clearFrom = 0;
      } else {
         clearFrom = fromInclusive;
      }
      Arrays.fill(elements, clearFrom, toExclusive, null);
      removed += (toExclusive - clearFrom);
         // Need to also catch up to next non null value
      headSequence = catchUpHeadPointer(elements, indexExclusive, tailSequence);
      return removed;
   }

   private int bufferOffset(final long index) {
      return bufferOffset(index, elements.length);
   }

   private static int bufferOffset(final long index, final int elementsLength) {
      return (int) (index & (elementsLength - 1));
   }

   public T get(final long index) {
      validateIndex(index);
      return elements[bufferOffset(index)];
   }

   private T replace(final long index, final T e) {
      final int offset = bufferOffset(index);
      final T oldValue = elements[offset];
      elements[offset] = e;
      return oldValue;
   }

   public T set(final long index, final T e) {
      Objects.requireNonNull(e);
      validateIndex(index, false);
      if (index < tailSequence) {
         return replace(index, e);
      }
      final int requiredCapacity = (int) (index - tailSequence) + 1;
      final int missingCapacity = requiredCapacity - availableCapacityWithoutResizing();
      if (missingCapacity > 0) {
         growCapacity(missingCapacity);
      }
      final T[] elements = this.elements;
      elements[bufferOffset(index)] = e;
      if (index >= tailSequence) {
         // If we were empty, move the head along
         if (headSequence == tailSequence) {
            headSequence = index;
         }
         tailSequence = index + 1;
      }
      return null;
   }

   public void add(final T e) {
      Objects.requireNonNull(e);
      if (availableCapacityWithoutResizing() == 0) {
         growCapacity(1);
      }
      final long index = tailSequence;
      elements[bufferOffset(index)] = e;
      tailSequence = index + 1;
   }

   public boolean isEmpty() {
      return tailSequence == headSequence;
   }

   public T peek() {
      if (isEmpty()) {
         return null;
      }
      return get(headSequence);
   }

   /** Removes the element at head, does not catch up the headSequence but will increment */
   public T poll() {
      if (isEmpty()) {
         return null;
      }
      final T[] elements = this.elements;
      long headSequence = this.headSequence;
      final int offset = bufferOffset(headSequence);
      final T e = elements[offset];
      elements[offset] = null;
      this.headSequence = catchUpHeadPointer(elements, ++headSequence, tailSequence);
      return e;
   }

   /** Removes an element at index */
   public T remove(long index) {
      if(isEmpty() || !contains(index))
         return null;
      final T[] elements = this.elements;
      final int offset = bufferOffset(index);
      final T e = elements[offset];
      elements[offset] = null;
      long headSequence = this.headSequence;
      if (index == headSequence) {
         // how about removal at tailSequence? tailSequence-- ?
         this.headSequence = catchUpHeadPointer(elements, ++headSequence, tailSequence);
      }
      return e;
   }

   private static <T> long catchUpHeadPointer(final T[] elements, long headSequence, long tailSequence) {
      while (headSequence < tailSequence) {
         int innerOffset = bufferOffset(headSequence, elements.length);
         if (elements[innerOffset] != null) {
            break;
         }
         headSequence++;
      }
      return headSequence;
   }

   public String toString() {
      return String.format("[%s..%s] (%s elements)", headSequence, tailSequence, size(false));
   }

   private void growCapacity(int delta) {
      assert delta > 0;
      final T[] oldElements = this.elements;
      final int newCapacity = findNextPositivePowerOfTwo(oldElements.length + delta);
      if (newCapacity < 0) {
         // see ArrayList::newCapacity
         throw new OutOfMemoryError();
      }
      final T[] newElements = allocate(newCapacity);
      final int size = size();
      final long headSequence = this.headSequence;
      long oldIndex = headSequence;
      long newIndex = headSequence;
      int remaining = size;
      while (remaining > 0) {
         final int fromOldIndex = bufferOffset(oldIndex, oldElements.length);
         final int fromNewIndex = bufferOffset(newIndex, newCapacity);
         final int toOldEnd = oldElements.length - fromOldIndex;
         final int toNewEnd = newElements.length - fromNewIndex;
         final int bytesToCopy = Math.min(Math.min(remaining, toOldEnd), toNewEnd);
         System.arraycopy(oldElements, fromOldIndex, newElements, fromNewIndex, bytesToCopy);
         oldIndex += bytesToCopy;
         newIndex += bytesToCopy;
         remaining -= bytesToCopy;
      }
      this.elements = newElements;
   }

   private static int findNextPositivePowerOfTwo(final int value) {
      return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
   }

   @SuppressWarnings("unchecked")
   private static <T> T[] allocate(int capacity) {
      return (T[]) (capacity == 0 ? EMPTY : new Object[capacity]);
   }
}
