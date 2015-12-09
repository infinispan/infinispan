package org.infinispan.objectfilter.impl.aggregation;


/**
 * Computes the average of doubles. The implementation uses compensated summation in order to reduce the error bound in
 * the numerical sum compared to a simple summation of {@code double} values, similar to the way {@link
 * java.util.DoubleSummaryStatistics} works.
 *
 * @author anistor@redhat.com
 * @since 8.1
 */
class DoubleAvg extends DoubleSum {

   private long count;

   void update(double value) {
      update(value, 1);
   }

   void update(double value, long count) {
      super.update(value);
      this.count += count;
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

   public long getCount() {
      return count;
   }
}
