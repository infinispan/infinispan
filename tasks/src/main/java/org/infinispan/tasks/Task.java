package org.infinispan.tasks;

public interface Task {
   /**
    * Returns the name of the task
    */
   String getName();

   /**
    * Returns the type of task. This is dependent on the specific implementation.
    */
   String getType();

   /**
    * Returns the execution mode in which this task can be executed
    */
   TaskExecutionMode getExecutionMode();

}
