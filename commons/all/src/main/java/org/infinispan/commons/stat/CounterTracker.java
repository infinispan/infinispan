package org.infinispan.commons.stat;

/**
 * Tracks a monotonically increasing values.
 * <p>
 * The counters may never be reset to a lesser value.
 */
public interface CounterTracker {

   /**
    * Update the counter by one.
    */
   void increment();

   /**
    * Update the counter by {@code amount}.
    *
    * @param amount The amount to add to the counter.
    */
   void increment(double amount);

}
