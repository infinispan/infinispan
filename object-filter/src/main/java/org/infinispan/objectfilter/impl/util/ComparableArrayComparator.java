package org.infinispan.objectfilter.impl.util;

import java.util.Comparator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ComparableArrayComparator implements Comparator<Comparable[]> {

   private final boolean[] direction;

   public ComparableArrayComparator(boolean[] direction) {
      this.direction = direction;
   }

   @Override
   public int compare(Comparable[] array1, Comparable[] array2) {
      if (array1 == null || array2 == null) {
         throw new IllegalArgumentException("arguments cannot be null");
      }
      if (array1.length != direction.length || array2.length != direction.length) {
         throw new IllegalArgumentException("arrays must have the same size");
      }

      for (int i = 0; i < array1.length; i++) {
         int r = compareElements(array1[i], array2[i], direction[i]);
         if (r != 0) {
            return r;
         }
      }
      return 0;
   }

   private int compareElements(Comparable o1, Comparable o2, boolean isAsc) {
      if (o1 == null) {
         return o2 == null ? 0 : isAsc ? -1 : 1;
      } else if (o2 == null) {
         return isAsc ? 1 : -1;
      }
      int r = o1.compareTo(o2);
      return isAsc ? r : -r;
   }
}
