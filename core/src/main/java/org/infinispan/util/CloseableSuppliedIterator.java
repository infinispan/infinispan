package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class CloseableSuppliedIterator<E> implements CloseableIterator<E> {
   private final CloseableSupplier<? extends E> supplier;

   private E next;

   public CloseableSuppliedIterator(CloseableSupplier<? extends E> supplier) {
      if (supplier == null) {
         throw new NullPointerException();
      }
      this.supplier = supplier;
   }

   @Override
   public void close() {
      supplier.close();
   }

   private E getNext() {
      return supplier.get();
   }

   @Override
   public boolean hasNext() {
      if (next == null) {
         next = getNext();
      }
      return next != null;
   }

   @Override
   public E next() {
      E e = next == null ? getNext() : next;
      if (e == null) {
         throw new NoSuchElementException();
      }
      next = null;
      return e;
   }

   @Override
   public void forEachRemaining(Consumer<? super E> action) {
      E supplied;
      if (next != null) {
         action.accept(next);
      }
      while ((supplied = supplier.get()) != null) {
         action.accept(supplied);
      }
   }
}
