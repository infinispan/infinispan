package org.infinispan.partitionhandling;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
* @author Mircea Markus
* @author Dan Berindei
* @since 7.0
*/
@Proto
@ProtoTypeId(ProtoStreamTypeIds.AVAILABILITY_MODE)
public enum AvailabilityMode {

   /**
    * Regular operation mode
    */
   AVAILABLE,

   /**
    * Data can not be safely accessed because of a network split or successive nodes leaving.
    */
   DEGRADED_MODE;

   public AvailabilityMode min(AvailabilityMode other) {
      if (this == DEGRADED_MODE || other == DEGRADED_MODE)
         return DEGRADED_MODE;
      return AVAILABLE;
   }
}
