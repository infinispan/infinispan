package org.infinispan.objectfilter.impl.util;

/**
 * Represents an interval of values of type K. K is not restricted to be a Comparable type here, but the IntervalTree
 * must be able to compare values of type K using its Comparator.
 *
 * @param <K> the key value type
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class Interval<K extends Comparable<K>> {

   public static <K extends Comparable<K>> K getMinusInf() {
      return (K) MINUS_INF;
   }

   public static <K extends Comparable<K>> K getPlusInf() {
      return (K) PLUS_INF;
   }

   /**
    * Placeholder for the smallest possible value.
    */
   private static final Comparable MINUS_INF = new Comparable() {
      @Override
      public String toString() {
         return "-INF";
      }

      @Override
      public int compareTo(Object obj) {
         return obj == this ? 0 : -1;
      }

      @Override
      public boolean equals(Object obj) {
         return obj == this;
      }
   };

   /**
    * Placeholder for the greatest possible value.
    */
   private static final Comparable PLUS_INF = new Comparable() {
      @Override
      public String toString() {
         return "+INF";
      }

      @Override
      public int compareTo(Object obj) {
         return obj == this ? 0 : 1;
      }

      @Override
      public boolean equals(Object obj) {
         return obj == this;
      }
   };

   /**
    * The lower bound.
    */
   public final K low;

   /**
    * Indicates if the interval is closed in the lower bound.
    */
   public final boolean includeLower;

   /**
    * The upper bound.
    */
   public final K up;

   /**
    * Indicates if the interval is closed in the upper bound.
    */
   public final boolean includeUpper;

   public Interval(K low, boolean includeLower, K up, boolean includeUpper) {
      if (low == null || up == null) {
         throw new IllegalArgumentException("arguments cannot be null");
      }
      this.low = low;
      this.includeLower = includeLower;
      this.up = up;
      this.includeUpper = includeUpper;
   }

   public boolean contains(K value) {
      return (includeLower ? low.compareTo(value) <= 0 : low.compareTo(value) < 0)
            && (includeUpper ? up.compareTo(value) >= 0 : up.compareTo(value) > 0);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Interval other = (Interval) obj;
      return includeLower == other.includeLower
            && includeUpper == other.includeUpper
            && low.equals(other.low) && up.equals(other.up);
   }

   @Override
   public int hashCode() {
      int result = low.hashCode();
      result = 31 * result + (includeLower ? 1 : 0);
      result = 31 * result + up.hashCode();
      result = 31 * result + (includeUpper ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return (includeLower ? "[" : "(") + low + ", " + up + (includeUpper ? "]" : ")");
   }
}