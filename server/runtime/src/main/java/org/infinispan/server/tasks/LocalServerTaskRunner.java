package org.infinispan.server.tasks;

import java.util.concurrent.CompletableFuture;

import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public final class LocalServerTaskRunner implements ServerTaskRunner {

   private static final LocalServerTaskRunner INSTANCE = new LocalServerTaskRunner();

   private LocalServerTaskRunner() { }

   public static LocalServerTaskRunner getInstance() {
      return INSTANCE;
   }

   @Override
   public <T> CompletableFuture<T> execute(ServerTaskWrapper<T> task, TaskContext context) {
      try {
         BlockingManager bm = SecurityActions.getGlobalComponentRegistry(context.getCacheManager())
               .getComponent(BlockingManager.class);
         return bm.supplyBlocking(() -> Security.doAs(context.subject(), task, context), "local-task-" + task.getName())
               .toCompletableFuture();
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
   }
}
