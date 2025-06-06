package org.infinispan.util.logging.events;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;

/**
 * EventLogCategory.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.EVENT_LOG_CATEGORY)
public enum EventLogCategory {

   @ProtoEnumValue(number = 0)
   LIFECYCLE,

   @ProtoEnumValue(number = 1)
   CLUSTER,

   @ProtoEnumValue(number = 2)
   SECURITY,

   @ProtoEnumValue(number = 3)
   TASKS;

   private final Logger logger;

   EventLogCategory() {
      this.logger = LogFactory.getLogger(this.toString());
   }

   public Logger logger() {
      return logger;
   }
}
