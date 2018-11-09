package org.infinispan.commons.stat;

/**
 * A simple statistic recorder that computes the average, minimum and maximum value observed.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public interface SimpleStat {

   default void record(long value) {
   }

   default long getMin(long defaultValue) {
      return defaultValue;
   }

   default long getMax(long defaultValue) {
      return defaultValue;
   }

   default long getAverage(long defaultValue) {
      return defaultValue;
   }

   default long count() {
      return 0;
   }

   default boolean isEmpty() {
      return count() == 0;
   }

}
