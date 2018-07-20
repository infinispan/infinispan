package org.infinispan.scripting.impl;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * ScriptExecutionMode.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
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
