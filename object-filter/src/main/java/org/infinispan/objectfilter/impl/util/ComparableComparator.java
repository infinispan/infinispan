package org.infinispan.objectfilter.impl.util;

import java.util.Comparator;

/**
 * Trivial Comparator implementation that expects the objects to be compared to implement Comparable themselves.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ComparableComparator<T extends Comparable<T>> implements Comparator<T> {

   @Override
   public int compare(T o1, T o2) {
      return o1.compareTo(o2);
   }
}
