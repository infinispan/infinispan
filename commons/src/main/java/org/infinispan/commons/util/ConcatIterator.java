package org.infinispan.commons.util;

import java.util.Iterator;

/**
 * Iterator that concatenates a bunch of iterables into 1 big iterator. Each iterable is retrieved lazily as requested.
 * Note that if any of the produced iterators from the iterable implement {@link CloseableIterator} they will be closed
 * when iterated upon fully or the last used iterator will be close when this iterator this closed.
 * <p>
 * Removal is implemented and will invoke remove on the last used iterator
 * @author wburns
 * @since 9.3
 */
public class ConcatIterator<E> extends AbstractIterator<E> implements CloseableIterator<E> {
   private final Iterator<? extends Iterable<E>> iterables;

   private Iterator<E> currentIterator;

   public ConcatIterator(Iterable<? extends Iterable<E>> iterableIterables) {
      this.iterables = iterableIterables.iterator();
      if (iterables.hasNext()) {
         // Always initialize current iterator
         currentIterator = iterables.next().iterator();
      }
   }

   @Override
   protected E getNext() {
      E next = null;
      if (currentIterator != null) {
         while (next == null) {
            if (currentIterator.hasNext()) {
               next = currentIterator.next();
            } else {
               closeIterator(currentIterator);
               if (iterables.hasNext()) {
                  currentIterator = iterables.next().iterator();
               } else {
                  // None more left
                  currentIterator = null;
                  break;
               }
            }
         }
      }
      return next;
   }

   @Override
   public void close() {
      closeIterator(currentIterator);
   }

   @Override
   public void remove() {
      if (currentIterator != null) {
         currentIterator.remove();
      }
   }

   private static void closeIterator(Iterator<?> iter) {
      if (iter instanceof CloseableIterator) {
         ((CloseableIterator) iter).close();
      }
   }
}
