package org.infinispan.tasks;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.tasks.spi.TaskEngine;

/**
 * TaskManager. Allows executing tasks and retrieving the list of currently running tasks
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface TaskManager {

   /**
    * Executes the named task, passing an optional cache and a map of named parameters.
    *
    * @param taskName
    * @param context
    * @return
    */
   <T> CompletionStage<T> runTask(String taskName, TaskContext context);

   /**
    * Retrieves the currently executing tasks. If running in a cluster this operation
    * will return all of the tasks running on all nodes
    *
    * @return a list of {@link TaskExecution} elements
    */
   List<TaskExecution> getCurrentTasks();

   /**
    * Retrieves the installed task engines
    */
   List<TaskEngine> getEngines();

    /**
     * Retrieves the list of all available tasks
     *
     * @return a list of {@link Task} elements
     */
   List<Task> getTasks();

   /**
    * Same as {@link #getTasks()} except that the tasks are returned in a non
    * blocking fashion.
    * @return a stage that when complete contains all the tasks available
    */
   CompletionStage<List<Task>> getTasksAsync();

   /**
    *
    * @return Retrieves the list of all available tasks, excluding administrative tasks with names starting with '@@'
    */
   List<Task> getUserTasks();

   /**
    * Same as {@link #getTasks()} except that the user tasks are returned in a non
    * @return a stage that when complete contains all the user tasks available
    */
   CompletionStage<List<Task>> getUserTasksAsync();

   /**
    * Registers a new {@link TaskEngine}
    *
    * @param taskEngine an instance of the task engine that has to be registered with the task manager
    */
   void registerTaskEngine(TaskEngine taskEngine);
}
