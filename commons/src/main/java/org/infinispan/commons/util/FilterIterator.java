package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Iterator that also filters out entries based on the provided predicate. This iterator implements
 * {@link CloseableIterator} and will close the provided iterator if it also implemented CloseableIterator.
 * <p>
 * This iterator supports removal as long as the provided iterator supports removal. Although note only entries
 * returned by the filter can be removed.
 * @author wburns
 * @since 9.3
 */
public class FilterIterator<E> extends AbstractIterator<E> implements CloseableIterator<E> {
   private final Iterator<E> iter;
   private final Predicate<? super E> filter;

   public FilterIterator(Iterator<E> iter, Predicate<? super E> filter) {
      this.iter = iter;
      this.filter = filter;
   }

   @Override
   protected E getNext() {
      while (iter.hasNext()) {
         E next = iter.next();
         if (filter.test(next)) {
            return next;
         }
      }
      return null;
   }

   @Override
   public void close() {
      if (iter instanceof CloseableIterator) {
         ((CloseableIterator) iter).close();
      }
   }

   @Override
   public void remove() {
      iter.remove();
   }
}
