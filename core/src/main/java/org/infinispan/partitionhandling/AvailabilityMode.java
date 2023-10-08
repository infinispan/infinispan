package org.infinispan.partitionhandling;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
* @author Mircea Markus
* @author Dan Berindei
* @since 7.0
*/
@ProtoTypeId(ProtoStreamTypeIds.AVAILABILITY_MODE)
public enum AvailabilityMode {

   /**
    * Regular operation mode
    */
   @ProtoEnumValue(number = 1)
   AVAILABLE,

   /**
    * Data can not be safely accessed because of a network split or successive nodes leaving.
    */
   @ProtoEnumValue(number = 2)
   DEGRADED_MODE;

   public AvailabilityMode min(AvailabilityMode other) {
      if (this == DEGRADED_MODE || other == DEGRADED_MODE)
         return DEGRADED_MODE;
      return AVAILABLE;
   }

   private static final AvailabilityMode[] CACHED_VALUES = values();

   public static AvailabilityMode valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }
}
