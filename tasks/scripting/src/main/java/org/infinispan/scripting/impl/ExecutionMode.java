package org.infinispan.scripting.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * ScriptExecutionMode.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.EXECUTION_MODE)
public enum ExecutionMode {

   @ProtoEnumValue(number = 1)
   LOCAL(LocalRunner.INSTANCE, false),

   @ProtoEnumValue(number = 2)
   DISTRIBUTED(DistributedRunner.INSTANCE, true);

   private final ScriptRunner runner;
   private final boolean clustered;

   private ExecutionMode(ScriptRunner runner, boolean clustered) {
      this.runner = runner;
      this.clustered = clustered;
   }

   public ScriptRunner getRunner() {
      return runner;
   }

   public boolean isClustered() {
      return clustered;
   }
}
