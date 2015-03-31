package org.infinispan.distribution.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.util.AbstractDelegatingSet;

/**
 * Set implementation that shows a read only view of the provided set by only allowing
 * entries that map to a given segment using the provided consistent hash.
 * <p>
 * This set is useful when you don't want to copy an entire set but only need to see
 * values from the given segments.
 * <p>
 * Note many operations are not constant time when using this set.  Please check the method
 * you are using to see if it will perform differently than normally expected.
 * @author wburns
 * @since 7.2
 */
public class ReadOnlySegmentAwareSet<E> extends AbstractDelegatingSet<E> {

   protected final Set<E> set;
   protected final ConsistentHash ch;
   protected final Set<Integer> allowedSegments;

   public ReadOnlySegmentAwareSet(Set<E> set, ConsistentHash ch, Set<Integer> allowedSegments) {
      super();
      this.set = Collections.unmodifiableSet(set);
      this.ch = ch;
      this.allowedSegments = allowedSegments;
   }

   @Override
   protected Set<E> delegate() {
      return set;
   }

   protected boolean valueAllowed(Object obj) {
      int segment = ch.getSegment(obj);
      return allowedSegments.contains(segment);
   }

   @Override
   public boolean contains(Object o) {
      if (valueAllowed(o)) {
         return super.contains(o);
      }
      return false;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      for (Object obj : c) {
         if (valueAllowed(obj) && !super.contains(obj)) {
            return false;
         }
      }
      return true;
   }

   /**
    * Checks if the provided set is empty.  This is done by iterating over all of the values
    * until it can find a key that maps to a given segment.
    * <p>
    * This method should always be preferred over checking the size to see if it is empty.
    * <p>
    * This time complexity for this method between O(1) to O(N).
    */
   @Override
   public boolean isEmpty() {
      Iterator<E> iter = iterator();
      return !iter.hasNext();
   }

   /**
    * Returns the size of the read only set.  This is done by iterating over all of the
    * values counting all that are in the segments.
    * <p>
    * If you are using this method to verify if the set is empty, you should instead use
    * the {@link ReadOnlySegmentAwareEntrySet#isEmpty()} as it will perform better if the
    * size is only used for this purpose.
    * <p>
    * This time complexity for this method is always O(N).
    */
   @Override
   public int size() {
      Iterator<E> iter = iterator();
      int count = 0;
      while (iter.hasNext()) {
         iter.next();
         count++;
      }
      return count;
   }

   @Override
   public Iterator<E> iterator() {
      return new ReadOnlySegmentAwareIterator<>(super.iterator(), ch, allowedSegments);
   }
}
