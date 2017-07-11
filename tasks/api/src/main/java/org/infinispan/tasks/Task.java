package org.infinispan.tasks;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public interface Task {
   /**
    * Provides a name for the task. This is the name by which the task will be executed.
    * Make sure the name is unique for task.
    *
    * @return name of the server task
    */
   String getName();

   /**
    * Returns the type of task. This is dependent on the specific implementation.
    */
   String getType();

   /**
    * Provides info whether the task execution should be local - on one node or distributed - on all nodes.
    *
    * ONE_NODE execution is the default.
    *
    * @return {@link TaskExecutionMode#ONE_NODE} for single node execution, {@link TaskExecutionMode#ALL_NODES} for distributed execution,
    */
   default TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ONE_NODE;
   }

   /**
    * The named parameters accepted by this task
    *
    * @return a java.util.Set of parameter names
    */
   default Set<String> getParameters() {
      return Collections.emptySet();
   }

   /**
    * An optional role, for which the task is accessible.
    * If the task executor has the role in the set, the task will be executed.
    *
    * If the role is not provided - all users can invoke the task
    *
    * @return a user role, for which the task can be executed
    */
   default Optional<String> getAllowedRole() {
      return Optional.empty();
   }
}
