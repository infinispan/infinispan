package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Abstract iterator that allows overriding just the {@link AbstractIterator#getNext()} method to implement it.
 * This iterator works on the premise that a null value returned from {@link AbstractIterator#getNext()} means
 * that this iterator is complete.
 * Note this iterator does not implement {@link Iterator#remove()} and is up to implementors to do so.
 * @author wburns
 * @since 9.2
 */
public abstract class AbstractIterator<E> implements Iterator<E> {
   E next;

   /**
    * Method to implement to provide an iterator implementation. When this method returns null, the iterator is complete.
    * @return the next value for the iterator to return or null for it to be complete.
    */
   protected abstract E getNext();

   @Override
   public boolean hasNext() {
      return next != null || (next = getNext()) != null;
   }

   @Override
   public E next() {
      if (hasNext()) {
         E returnValue = next;
         next = null;
         return returnValue;
      }
      throw new NoSuchElementException();
   }

   @Override
   public void forEachRemaining(Consumer<? super E> action) {
      if (next != null) {
         action.accept(next);
      }
      E next;
      while ((next = getNext()) != null) {
         action.accept(next);
      }
   }
}
