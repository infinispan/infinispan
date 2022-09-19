package org.infinispan.tasks;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public interface Task extends JsonSerialization {
   /**
    * Provides a name for the task. This is the name by which the task will be executed.
    * Make sure the name is unique for each task.
    *
    * @return name of the task
    */
   String getName();

   /**
    * Returns the type of task. This is dependent on the specific implementation.
    */
   String getType();

   /**
    * Whether the task execution should be local - on one node or distributed - on all nodes.
    *
    * ONE_NODE execution is the default.
    *
    * @return {@link TaskExecutionMode#ONE_NODE} for single node execution, {@link TaskExecutionMode#ALL_NODES} for distributed execution,
    */
   default TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ONE_NODE;
   }

   /**
    * Whether tasks should reuse a single instance or create a new instance per execution. {@link TaskInstantiationMode#SHARED} is the default
    *
    * @return TaskInstantiationMode
    */
   default TaskInstantiationMode getInstantiationMode() {
      return TaskInstantiationMode.SHARED;
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

   @Override
   default Json toJson() {
      return Json.object()
            .set("name", getName())
            .set("type", getType())
            .set("parameters", Json.make(getParameters()))
            .set("execution_mode", getExecutionMode().toString())
            .set("instantiation_mode", getInstantiationMode().toString())
            .set("allowed_role", getAllowedRole().orElse(null));
   }
}
