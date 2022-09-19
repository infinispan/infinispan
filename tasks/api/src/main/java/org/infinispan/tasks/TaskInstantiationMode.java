package org.infinispan.tasks;

public enum TaskInstantiationMode {
   /**
    * Creates a single instance that is reused for every task execution
    */
   SHARED,
   /**
    * Creates a new instance for every invocation
    */
   ISOLATED
}
