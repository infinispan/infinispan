package org.infinispan.api.common.events;

/**
 * The possible states for a counter value.
 *
 * @since 14.0
 */
public enum CounterState {
   /**
    * The counter value is valid.
    */
   VALID,

   /**
    * The counter value has reached its min threshold.
    */
   LOWER_BOUND_REACHED,

   /**
    * The counter value has reached its max threshold.
    */
   UPPER_BOUND_REACHED;

   private static final CounterState[] CACHED_VALUES = CounterState.values();

   public static CounterState valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
