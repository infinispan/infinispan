package org.infinispan.util.logging.events;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * EventLogCategory.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
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
