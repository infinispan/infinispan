package org.infinispan.server.test.task.servertask;


import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;


/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 6:33 AM
 */
public class LocalTestServerTask implements ServerTask {

   public static final String NAME = "serverTask8234892";
   public static final String TASK_EXECUTED = "taskExecuted";
   public static final String CACHE_NAME = "taskAccessible";
   public static final String MODIFIED_PREFIX = "modified:";
   private TaskContext taskContext;

   @Override
   @SuppressWarnings("unchecked")
   public Object call() throws IOException, ClassNotFoundException {
      Cache<Object, Object> cache = (Cache<Object, Object>) taskContext.getCache().get();
      Map.Entry<Object, Object> entry = cache.entrySet().iterator().next();

      cache.getCacheManager().getCache(CACHE_NAME).put(entry.getKey(), MODIFIED_PREFIX + entry.getValue());

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
