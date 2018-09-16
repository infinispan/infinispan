package org.infinispan.tasks;


import java.util.concurrent.Callable;

/**
 * An interface for deployed server tasks.
 * In order to deploy a custom ServerTask, deploy a module containing a service that implements this interface.
 *
 * The task will be accessible by the name returned by {@link #getName()}
 * Before the execution, {@link TaskContext} is injected into the task to provide
 * {@link org.infinispan.manager.EmbeddedCacheManager}, {@link org.infinispan.Cache},
 * {@link org.infinispan.commons.marshall.Marshaller} and parameters.
 *
 * @author Michal Szynkiewicz &lt;michal.l.szynkiewicz@gmail.com&gt;
 */
public interface ServerTask<V> extends Callable<V>, Task {
   /**
    * Sets the task context
    * Store the value in your task implementation to be able to access caches and other resources in the task
    *
    * @param taskContext task execution context
    */
   void setTaskContext(TaskContext taskContext);

   default String getType() {
      return ServerTask.class.getSimpleName();
   }
}
