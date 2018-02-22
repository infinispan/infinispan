package org.infinispan.server.infinispan.task;

import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.tasks.TaskContext;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 9:36 AM
 */
public class LocalServerTaskRunner implements ServerTaskRunner {

   @Override
   public <T> CompletableFuture<T> execute(String taskName, TaskContext context) {
      ServerTaskWrapper<T> task = getRegistry(context).getTask(taskName);
      try {
         task.inject(context);
         return CompletableFuture.completedFuture(task.run());
      } catch (Exception e) {
         CompletableFuture<T> finishedWithException = new CompletableFuture<>();
         finishedWithException.completeExceptionally(e);
         return finishedWithException;
      }
   }

   private ServerTaskRegistry getRegistry(TaskContext context) {
      Cache<?, ?> cache = context.getCache().get();
      return cache.getCacheManager().getGlobalComponentRegistry().getComponent(ServerTaskRegistry.class);
   }
}
