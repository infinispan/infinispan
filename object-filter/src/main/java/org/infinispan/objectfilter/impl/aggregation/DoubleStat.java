package org.infinispan.objectfilter.impl.aggregation;


/**
 * Computes the sum and average of doubles. The implementation uses compensated summation in order to reduce the error bound in the
 * numerical sum compared to a simple summation of {@code double} values, similar to the way {@link
 * java.util.DoubleSummaryStatistics} works.
 *
 * @author anistor@redhat.com
 * @since 8.1
 */
final class DoubleStat {

   private long count;
   private double sum;
   private double sumCompensation; // Low order bits of sum
   private double simpleSum;       // Used to compute right sum for non-finite inputs

   void update(double value) {
      update(value, 1);
   }

   void update(double value, long count) {
      this.count += count;
      simpleSum += value;

      // Incorporate a new double value using Kahan summation / compensated summation.
      double tmp = value - sumCompensation;
      double velvel = sum + tmp; // Little wolf of rounding error
      sumCompensation = (velvel - sum) - tmp;
      sum = velvel;
   }

   /**
    * Returns the sum of seen values. If any value is a NaN or the sum is at any point a NaN then the average will be
    * NaN. The average returned can vary depending upon the order in which values are seen.
    *
    * @return the sum of values
    */
   Double getSum() {
      if (count == 0) {
         return null;
      }
      // Better error bounds to add both terms as the final sum
      double tmp = sum + sumCompensation;
      if (Double.isNaN(tmp) && Double.isInfinite(simpleSum)) {
         // If the compensated sum is spuriously NaN from
         // accumulating one or more same-signed infinite values,
         // return the correctly-signed infinity stored in simpleSum.
         return simpleSum;
      } else {
         return tmp;
      }
   }

   /**
    * Returns the arithmetic mean of seen values, or null if no values have been seen. If any value is a NaN or the sum
    * is at any point a NaN then the average will be NaN. The average returned can vary depending upon the order in
    * which values are seen.
    *
    * @return the arithmetic mean of values, or null if none
    */
   Double getAvg() {
      return count == 0 ? null : getSum() / count;
   }

   long getCount() {
      return count;
   }
}
