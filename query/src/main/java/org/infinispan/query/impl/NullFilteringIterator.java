package org.infinispan.query.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;

import net.jcip.annotations.NotThreadSafe;

/**
 * An iterator wrapper that filters out (skips over) any null values returned by the wrapped iterator.
 *
 * @author Marko Luksa
 */
@NotThreadSafe
class NullFilteringIterator<E> implements Iterator<E> {

   protected final Iterator<E> delegate;

   private E next;

   NullFilteringIterator(Iterator<E> delegate) {
      this.delegate = delegate;
   }

   @Override
   public boolean hasNext() {
      if (next != null) {
         return true;
      }

      while (delegate.hasNext()) {
         next = delegate.next();
         if (next != null) {
            return true;
         }
      }
      return false;
   }

   @Override
   public E next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      try {
         return next;
      } finally {
         next = null;
      }
   }
}
