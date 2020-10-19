package org.infinispan.server.tasks;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_SERVER_TASK_PARAMETER)
class TaskParameter {
   @ProtoField(1)
   String key;

   @ProtoField(2)
   String value;

   @ProtoFactory
   TaskParameter(String key, String value) {
      this.key = key;
      this.value = value;
   }
}
