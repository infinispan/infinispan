package org.infinispan.server.tasks;

import java.util.concurrent.CompletableFuture;

import org.infinispan.tasks.TaskContext;

/**
 * Used by ServerTaskEngine to executed ServerTasks
 *
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public interface ServerTaskRunner {
   /**
    * Trigger execution of a ServerTask with given name. Returns a CompletableFuture, from which the result of execution
    * can be obtained.
    *
    * @param taskName name of the task to be executed
    * @param context  task context injected into task upon execution
    * @param <T>      task return type
    * @return completable future providing a way to get the result
    */
   <T> CompletableFuture<T> execute(String taskName, TaskContext context);
}
