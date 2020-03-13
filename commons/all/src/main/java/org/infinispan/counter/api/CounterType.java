package org.infinispan.counter.api;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The counter types.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_TYPE)
public enum CounterType {
   /**
    * A strong consistent and unbounded counter. The counter will never throw a {@link
    * CounterOutOfBoundsException}.
    */
   @ProtoEnumValue(number = 0)
   UNBOUNDED_STRONG,
   /**
    * A strong consistent and bounded counter. The counter will throw {@link CounterOutOfBoundsException}
    * if the boundaries are reached. The upper and lower bound are inclusive.
    */
   @ProtoEnumValue(number = 1)
   BOUNDED_STRONG,
   /**
    * A weak consistent counter. It focus on write performance and its counter value is only calculated at read time.
    */
   @ProtoEnumValue(number = 2)
   WEAK;

   private static final CounterType[] CACHED_VALUES = values();

   public static CounterType valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
