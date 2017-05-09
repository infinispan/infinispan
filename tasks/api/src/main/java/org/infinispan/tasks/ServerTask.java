package org.infinispan.tasks;


import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * An interface for deployed server tasks.
 * In order to deploy a custom ServerTask, deploy a module containing a service that implements this interface.
 *
 * The task will be accessible by the name returned by {@link #getName()}
 * Before the execution, {@link TaskContext} is injected into the task to provide
 * {@link org.infinispan.Cache}, {@link org.infinispan.commons.marshall.Marshaller} and parameters.
 *
 *
 * <p/>
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/19/16
 * Time: 2:18 PM
 */
public interface ServerTask<V> extends Callable<V> {
   /**
    * Sets the task context
    * Store the value in your task implementation to be able to access caches and other resources in the task
    *
    * @param taskContext task execution context
    */
   void setTaskContext(TaskContext taskContext);

   /**
    * Provides a name for the task. This is the name by which the task will be executed.
    * Make sure the name is unique for task.
    *
    * @return name of the server task
    */
   String getName();

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

   /**
    * The named parameters accepted by this task
    *
    * @return a java.util.Set of parameter names
    */
   default Set<String> getParameters() {
      return Collections.emptySet();
   }
}
