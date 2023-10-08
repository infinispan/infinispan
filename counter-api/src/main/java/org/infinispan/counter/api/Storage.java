package org.infinispan.counter.api;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The storage mode of a counter.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_STORAGE)
public enum Storage {
   /**
    * The counter value is lost when the cluster is restarted/stopped.
    */
   @ProtoEnumValue(number = 0)
   VOLATILE,
   /**
    * The counter value is stored persistently and survives a cluster restart/stop.
    */
   @ProtoEnumValue(number = 1)
   PERSISTENT;
}
