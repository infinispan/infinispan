package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * A iterator that maps each value to the output of the Function.  Note that the remove is supported if the iterator
 * originally supported remove. This iterator implements {@link CloseableIterator} and will close the provided iterator
 * if it also implemented CloseableIterator.
 * @author William Burns
 * @since 8.0
 */
public class IteratorMapper<E, S> implements CloseableIterator<S> {
   private final Iterator<? extends E> iterator;
   private final Function<? super E, ? extends S> function;

   public IteratorMapper(Iterator<? extends E> iterator, Function<? super E, ? extends S> function) {
      if (iterator == null || function == null) {
         throw new NullPointerException();
      }
      this.iterator = iterator;
      this.function = function;
   }

   @Override
   public boolean hasNext() {
      return iterator.hasNext();
   }

   @Override
   public S next() {
      E value = iterator.next();
      return function.apply(value);
   }

   @Override
   public void remove() {
      iterator.remove();
   }

   @Override
   public void close() {
      if (iterator instanceof CloseableIterator) {
         ((CloseableIterator) iterator).close();
      }
   }
}
