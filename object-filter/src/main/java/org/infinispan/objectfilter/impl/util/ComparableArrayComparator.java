package org.infinispan.objectfilter.impl.util;

import java.util.Comparator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ComparableArrayComparator implements Comparator<Comparable<?>[]> {

   private final boolean[] direction;

   /**
    * Constructs a comparator based on a direction boolean array. The length of the array must match the {@link
    * Comparable} arrays we are supposed to handle.
    *
    * @param direction an array of booleans indicating direction (true indicates ascending order)
    */
   public ComparableArrayComparator(boolean[] direction) {
      if (direction == null) {
         throw new IllegalArgumentException("direction array cannot be null");
      }
      this.direction = direction;
   }

   @Override
   public int compare(Comparable<?>[] array1, Comparable<?>[] array2) {
      if (array1 == null || array2 == null) {
         throw new IllegalArgumentException("arguments cannot be null");
      }
      if (array1.length != direction.length || array2.length != direction.length) {
         throw new IllegalArgumentException("argument arrays must have the same size as the direction array");
      }

      for (int i = 0; i < direction.length; i++) {
         int r = compareElements(array1[i], array2[i]);
         if (r != 0) {
            return direction[i] ? r : -r;
         }
      }
      return 0;
   }

   private static int compareElements(Comparable o1, Comparable o2) {
      if (o1 == null) {
         return o2 == null ? 0 : -1;
      } else if (o2 == null) {
         return 1;
      }
      return o1.compareTo(o2);
   }
}
