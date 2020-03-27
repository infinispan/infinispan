package org.infinispan.tasks.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.concurrent.BlockingManager;

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
    * @param blockingManager the handler for when a task is to be invoked that could block
    * @return
    */
   <T> CompletionStage<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager);

   /**
    * Returns whether this task engine knows about a specified named task
    *
    * @param taskName
    * @return
    */
   boolean handles(String taskName);

}
