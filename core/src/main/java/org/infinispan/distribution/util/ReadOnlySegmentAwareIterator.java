package org.infinispan.distribution.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Iterator implementation that shows a read only view of the provided iterator by only
 * allowing values that map to a given segment using the provided consistent hash.
 * <p>
 * This iterator is used with the other various SegmentAware Collections such as
 * {@link ReadOnlySegmentAwareSet}
 * 
 * @author wburns
 * @since 7.2
 */
public class ReadOnlySegmentAwareIterator<E> implements Iterator<E> {
   protected final Iterator<E> iter;
   protected final ConsistentHash ch;
   protected final Set<Integer> allowedSegments;

   protected E next;

   public ReadOnlySegmentAwareIterator(Iterator<E> iter, ConsistentHash ch, Set<Integer> allowedSegments) {
      super();
      this.iter = iter;
      this.ch = ch;
      this.allowedSegments = allowedSegments;
      next = findNext();
   }

   protected boolean valueAllowed(Object obj) {
      int segment = ch.getSegment(obj);
      return allowedSegments.contains(segment);
   }

   protected E findNext() {
      while (iter.hasNext()) {
         E next = iter.next();
         if (valueAllowed(next)) {
            return next;
         }
      }
      return null;
   }

   @Override
   public boolean hasNext() {
      return next != null;
   }

   @Override
   public E next() {
      E prev = next;
      if (prev == null) {
         throw new NoSuchElementException();
      }
      next = findNext();
      return prev;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("remove");
  }
}
