package org.infinispan.counter.api;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * The possible states for a counter value.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum CounterState {

   /**
    * The counter value is valid.
    */
   @ProtoEnumValue(number = 1)
   VALID,

   /**
    * The counter value has reached its min threshold, i.e. no thresholds has been reached.
    */
   @ProtoEnumValue(number = 2)
   LOWER_BOUND_REACHED,

   /**
    * The counter value has reached its max threshold.
    */
   @ProtoEnumValue(number = 3)
   UPPER_BOUND_REACHED;

   private static final CounterState[] CACHED_VALUES = CounterState.values();

   public static CounterState valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
