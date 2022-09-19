package org.infinispan.tasks;


import java.util.concurrent.Callable;

/**
 * An interface representing a deployed server task.
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
    * Note that, if {@link Task#getInstantiationMode()} is {@link TaskInstantiationMode#SHARED} there will be single
    * instance of each ServerTask on each server so, if you expect concurrent invocations of a task, the
    * {@link TaskContext} should be stored in a {@link ThreadLocal} static field in your task. The TaskContext should
    * then be obtained during the task's {@link #call()} method and removed from the ThreadLocal. Alternatively use
    * {@link TaskInstantiationMode#ISOLATED}.
    *
    * @param taskContext task execution context
    */
   void setTaskContext(TaskContext taskContext);

   default String getType() {
      return ServerTask.class.getSimpleName();
   }
}
