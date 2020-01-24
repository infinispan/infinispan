package org.infinispan.server.extensions;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class DistributedHelloServerTask implements ServerTask {
   private TaskContext taskContext;

   @Override
   public void setTaskContext(TaskContext taskContext) {
      this.taskContext = taskContext;
   }

   @Override
   public Object call() {
      EmbeddedCacheManager cacheManager = taskContext.getCacheManager();
      Object greetee = taskContext.getParameters().get().get("greetee");
      return String.format("Hello %s from %s", greetee == null ? "world" : greetee, cacheManager.getAddress());
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }

   @Override
   public String getName() {
      return "dist-hello";
   }
}
