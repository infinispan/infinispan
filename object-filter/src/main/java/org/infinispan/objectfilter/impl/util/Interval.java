package org.infinispan.objectfilter.impl.util;

/**
 * Represents an interval of values of type K. K is not restricted to be a Comparable type here, but the IntervalTree
 * must be able to compare values of type K using its Comparator.
 *
 * @param <K> the key value type
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class Interval<K> {

   //todo [anistor] handle -INF and +INF more gracefully and replace the nulls in low/up

   public static <K> K getMinusInf() {
      return (K) MINUS_INF;
   }

   public static <K> K getPlusInf() {
      return (K) PLUS_INF;
   }

   /**
    * Placeholder for the smallest possible value.
    */
   static final Object MINUS_INF = new Object() {
      @Override
      public String toString() {
         return "-INF";
      }
   };

   /**
    * Placeholder for the greatest possible value.
    */
   static final Object PLUS_INF = new Object() {
      @Override
      public String toString() {
         return "+INF";
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
      this.low = low;
      this.includeLower = includeLower;
      this.up = up;
      this.includeUpper = includeUpper;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Interval other = (Interval) obj;
      return includeLower == other.includeLower
            && includeUpper == other.includeUpper
            && !(low != null ? !low.equals(other.low) : other.low != null)
            && !(up != null ? !up.equals(other.up) : other.up != null);
   }

   @Override
   public int hashCode() {
      int result = low != null ? low.hashCode() : 0;
      result = 31 * result + (includeLower ? 1 : 0);
      result = 31 * result + (up != null ? up.hashCode() : 0);
      result = 31 * result + (includeUpper ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return (includeLower ? "[" : "(") + low + ", " + up + (includeUpper ? "]" : ")");
   }
}