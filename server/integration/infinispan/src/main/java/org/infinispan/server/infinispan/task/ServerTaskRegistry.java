package org.infinispan.server.infinispan.task;

import java.util.List;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.Task;

/**
 * Server task registry. Stores server tasks that can be executed via {@link org.infinispan.tasks.TaskManager}
 *
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public interface ServerTaskRegistry {

   /**
    * Lists the registered server tasks.
    *
    * @return a list of server tasks
    */
   List<Task> getTasks();

   /**
    * Returns a {@link ServerTaskWrapper} for a task with given name. ServerTaskWrapper wraps {@link ServerTask} to make
    * it compatible with {@link Task}
    *
    * @param taskName task name (as returned by {@link ServerTask#getName()})
    * @param <T>      type of return value of the task
    * @return server task wrapper for task of given name
    */
   <T> ServerTaskWrapper<T> getTask(String taskName);

   /**
    * Checks if task with given name is registered in this registry.
    *
    * @param taskName name of the task ({@link ServerTask#getName())
    * @return true if task is registered, false otherwise
    */
   boolean handles(String taskName);

   /**
    * Register a ServerTask in the registry
    *
    * @param task server task to register
    * @param <T>  type of the return value of the task
    */
   <T> void addDeployedTask(ServerTask<T> task);

   /**
    * Unregister server task with given name
    *
    * @param name name of the task
    */
   void removeDeployedTask(String name);
}
