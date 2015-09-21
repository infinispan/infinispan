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
      super.update(value);
      count++;
   }

   /**
    * Returns the arithmetic mean of seen values, or null if no values have been seen. If any value is a NaN or the sum
    * is at any point a NaN then the average will be NaN. The average returned can vary depending upon the order in
    * which values are seen.
    *
    * @return the arithmetic mean of values, or null if none
    */
   Double getValue() {
      return count == 0 ? null : super.getValue() / count;
   }
}
