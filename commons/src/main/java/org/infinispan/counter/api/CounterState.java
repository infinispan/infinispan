package org.infinispan.counter.api;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The possible states for a counter value.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_STATE)
public enum CounterState {

   /**
    * The counter value is valid.
    */
   @ProtoEnumValue(number = 0)
   VALID,

   /**
    * The counter value has reached its min threshold, i.e. no thresholds has been reached.
    */
   @ProtoEnumValue(number = 1)
   LOWER_BOUND_REACHED,

   /**
    * The counter value has reached its max threshold.
    */
   @ProtoEnumValue(number = 2)
   UPPER_BOUND_REACHED;

   private static final CounterState[] CACHED_VALUES = CounterState.values();

   public static CounterState valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
