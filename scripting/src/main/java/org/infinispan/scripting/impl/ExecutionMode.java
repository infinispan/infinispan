package org.infinispan.scripting.impl;

/**
 * ScriptExecutionMode.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public enum ExecutionMode {
   LOCAL(LocalRunner.INSTANCE), DISTRIBUTED(DistributedRunner.INSTANCE), MAPPER(MapReduceRunner.INSTANCE), REDUCER(NullRunner.INSTANCE), COLLATOR(NullRunner.INSTANCE), COMBINER(
         NullRunner.INSTANCE);

   private final ScriptRunner runner;

   private ExecutionMode(ScriptRunner runner) {
      this.runner = runner;
   }

   public ScriptRunner getRunner() {
      return runner;
   }
}
