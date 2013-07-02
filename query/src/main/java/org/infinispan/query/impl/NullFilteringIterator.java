package org.infinispan.query.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator wrapper that filters out (skips over) any null values returned by the wrapped iterator.
 *
 * @author Marko Luksa
 */
public class NullFilteringIterator<E> implements Iterator<E> {

   private Iterator<E> delegate;
   private E next;

   public NullFilteringIterator(Iterator<E> delegate) {
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

   @Override
   public void remove() {
      throw new UnsupportedOperationException("remove() is not supported");
   }
}
