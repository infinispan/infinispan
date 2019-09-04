package org.infinispan.util.logging.events;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * EventLogCategory.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.EVENT_LOG_CATEGORY)
public enum EventLogCategory {

   @ProtoEnumValue(number = 1)
   LIFECYCLE,

   @ProtoEnumValue(number = 2)
   CLUSTER,

   @ProtoEnumValue(number = 3)
   SECURITY,

   @ProtoEnumValue(number = 4)
   TASKS
}
