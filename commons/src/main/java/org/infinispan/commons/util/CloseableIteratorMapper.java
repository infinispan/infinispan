package org.infinispan.commons.util;

import java.util.function.Function;

/**
 * A iterator that maps each value to the output of the Function.  Note that the remove is supported if the iterator
 * originally supported remove.
 * @author William Burns
 * @since 8.0
 */
public class CloseableIteratorMapper<E, S> extends IteratorMapper<E, S> implements CloseableIterator<S> {
   private final CloseableIterator<? extends E> iterator;

   public CloseableIteratorMapper(CloseableIterator<? extends E> iterator, Function<? super E, ? extends S> function) {
      super(iterator, function);
      this.iterator = iterator;
   }

   @Override
   public void close() {
      iterator.close();
   }
}
