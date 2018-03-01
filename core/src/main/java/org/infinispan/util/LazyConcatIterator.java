package org.infinispan.util;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import org.infinispan.commons.util.CloseableIterator;

/**
 * Iterator implementation that will return all entries from the first iterator. Upon completion of the first iterator
 * the supplier will generate an additional iterator that will then be used as a source for this iterator. After the
 * second iterator is consumed this iterator will also be completed.
 * @param <E> element type
 */
public class LazyConcatIterator<E> implements CloseableIterator<E> {
   private final CloseableIterator<E> iterator1;
   private final Supplier<? extends CloseableIterator<E>> supplier;

   private CloseableIterator<E> iterator2;

   public LazyConcatIterator(CloseableIterator<E> first, Supplier<? extends CloseableIterator<E>> supplier) {
      this.iterator1 = Objects.requireNonNull(first);
      this.supplier = Objects.requireNonNull(supplier);
   }

   @Override
   public void close() {
      try (CloseableIterator<E> closeme = iterator1) {
         if (iterator2 != null) {
            iterator2.close();
         }
      }
   }

   @Override
   public boolean hasNext() {
      boolean hasNext;
      if (iterator2 == null) {
         hasNext = iterator1.hasNext();
         if (hasNext) {
            return true;
         } else {
            iterator2 = supplier.get();
         }
      }

      return iterator2.hasNext();
   }

   @Override
   public E next() {
      if (iterator2 == null) {
         // We have to double check hasNext in case if they are calling next without hasNext
         if (iterator1.hasNext()) {
            return iterator1.next();
         } else {
            iterator2 = supplier.get();
         }
      }

      if (iterator2.hasNext()) {
         return iterator2.next();
      }
      throw new NoSuchElementException();
   }
}
