package org.infinispan.server.test.task;


import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

import java.util.Optional;
import java.util.Set;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 6:33 AM
 */
public class LocalTestServerTask implements ServerTask {

   public static final String NAME = "serverTask8234892";
   public static final String TASK_EXECUTED = "taskExecuted";
   private TaskContext taskContext;

   @Override
   @SuppressWarnings("unchecked")
   public Object call() {
      Cache<Object, Object> cache = (Cache<Object, Object>) taskContext.getCache().get();
      cache.getCacheManager().getCache("taskAccessible").put(TASK_EXECUTED, "true");
      return null;
   }

   @Override
   public void setTaskContext(TaskContext taskContext) {
      this.taskContext = taskContext;
   }

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public Optional<String> getAllowedRole() {
      return Optional.empty();
   }

}
