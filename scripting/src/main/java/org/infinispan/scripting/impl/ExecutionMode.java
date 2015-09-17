package org.infinispan.scripting.impl;

/**
 * ScriptExecutionMode.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public enum ExecutionMode {
   LOCAL(LocalRunner.INSTANCE, false),
   DISTRIBUTED(DistributedRunner.INSTANCE, true),
   @Deprecated
   MAPPER(MapReduceRunner.INSTANCE, true),
   @Deprecated
   REDUCER(NullRunner.INSTANCE, false),
   @Deprecated
   COLLATOR(NullRunner.INSTANCE, false),
   @Deprecated
   COMBINER(NullRunner.INSTANCE, false);

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
