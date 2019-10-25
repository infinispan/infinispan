package org.infinispan.server.tasks;

import java.util.concurrent.CompletableFuture;

import org.infinispan.tasks.TaskContext;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public class LocalServerTaskRunner implements ServerTaskRunner {

   private final ServerTaskEngine serverTaskEngine;

   public LocalServerTaskRunner(ServerTaskEngine serverTaskEngine) {
      this.serverTaskEngine = serverTaskEngine;
   }

   @Override
   public <T> CompletableFuture<T> execute(String taskName, TaskContext context) {
      ServerTaskWrapper<T> task = serverTaskEngine.getTask(taskName);
      try {
         task.inject(context);
         return CompletableFuture.completedFuture(task.run());
      } catch (Exception e) {
         CompletableFuture<T> finishedWithException = new CompletableFuture<>();
         finishedWithException.completeExceptionally(e);
         return finishedWithException;
      }
   }
}
