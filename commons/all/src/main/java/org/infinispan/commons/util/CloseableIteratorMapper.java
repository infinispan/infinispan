package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * A iterator that maps each value to the output of the Function.  Note that the remove is supported if the iterator
 * originally supported remove.
 * @author William Burns
 * @since 8.0
 * @deprecated since 9.3 users can just use {@link IteratorMapper} as it handles CloseableIterators now
 */
@Deprecated
public class CloseableIteratorMapper<E, S> extends IteratorMapper<E, S> implements CloseableIterator<S> {
   private final Iterator<? extends E> iterator;

   public CloseableIteratorMapper(Iterator<? extends E> iterator, Function<? super E, ? extends S> function) {
      super(iterator, function);
      this.iterator = iterator;
   }

   @Override
   public void close() {
      if (iterator instanceof CloseableIterator) {
         ((CloseableIterator) iterator).close();
      }
   }
}
