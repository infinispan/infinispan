package org.infinispan.tasks.spi;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;

/**
 * TaskEngine. An implementation of an engine for executing tasks. How the tasks are implemented is
 * dependent on the engine itself.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface TaskEngine {
   /**
    * Returns the name of the engine
    */
   String getName();

   /**
    * Returns the list of tasks managed by this engine
    *
    * @return
    */
   List<Task> getTasks();

   /**
    * Executes the named task on the specified cache, passing a map of named parameters.
    *
    * @param taskName the name of the task
    * @param context a task context
    * @param executor the executor which the can be used by the task engine to run the task
    * @return
    */
   <T> CompletableFuture<T> runTask(String taskName, TaskContext context, Executor executor);

   /**
    * Returns whether this task engine knows about a specified named task
    *
    * @param taskName
    * @return
    */
   boolean handles(String taskName);

}
