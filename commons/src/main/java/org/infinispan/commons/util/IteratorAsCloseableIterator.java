package org.infinispan.commons.util;

import java.util.Iterator;

/**
 * This is a simple bridge class to change a plain {@link java.util.Iterator} into a
 * {@link org.infinispan.commons.util.CloseableIterator} where the close method does nothing
 *
 * @author wburns
 * @since 7.0
 */
public class IteratorAsCloseableIterator<E> implements CloseableIterator<E> {
   private final Iterator<E> iterator;

   public IteratorAsCloseableIterator(Iterator<E> iterator) {
      this.iterator = iterator;
   }

   @Override
   public void close() {
      // This does nothing
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext();
   }

   @Override
   public E next() {
      return iterator.next();
   }

   @Override
   public void remove() {
      iterator.remove();
   }
}
