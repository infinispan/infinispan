package org.infinispan.tasks;

import java.util.Set;

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

   /**
    * Returns the list of named parameters for this task
    */
   Set<String> getParameters();

}
