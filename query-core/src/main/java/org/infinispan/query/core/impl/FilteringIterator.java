package org.infinispan.query.core.impl;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.infinispan.commons.util.CloseableIterator;

// TODO [anistor] Unused ??

/**
 * A {@link CloseableIterator} decorator that filters and transforms its elements.
 */
public final class FilteringIterator<S, T> implements CloseableIterator<T> {

   private final CloseableIterator<S> iterator;

   private final Function<? super S, ? extends T> function;

   private T nextResult = null;

   private boolean isReady = false;

   public FilteringIterator(CloseableIterator<S> iterator, Function<? super S, ? extends T> function) {
      this.iterator = iterator;
      this.function = function;
   }

   @Override
   public void close() {
      iterator.close();
   }

   @Override
   public boolean hasNext() {
      updateNext();
      return nextResult != null;
   }

   @Override
   public T next() {
      updateNext();
      if (nextResult != null) {
         T next = nextResult;
         isReady = false;
         nextResult = null;
         return next;
      } else {
         throw new NoSuchElementException();
      }
   }

   private void updateNext() {
      if (!isReady) {
         while (iterator.hasNext()) {
            S next = iterator.next();
            nextResult = function.apply(next);
            if (nextResult != null) {
               break;
            }
         }
         isReady = true;
      }
   }
}
