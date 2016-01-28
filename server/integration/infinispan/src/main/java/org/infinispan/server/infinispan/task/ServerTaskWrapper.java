package org.infinispan.server.infinispan.task;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

import java.util.Optional;
import java.util.Set;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 2:04 PM
 */
public class ServerTaskWrapper<T> implements Task {
   private final ServerTask<T> task;

   public ServerTaskWrapper(ServerTask<T> task) {
      this.task = task;
   }

   @Override
   public String getName() {
      return task.getName();
   }

   public T run() throws Exception {
      return task.call();
   }

   @Override
   public String getType() {
      return "Java";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return task.getExecutionMode();
   }

   public void inject(TaskContext context) {
      task.setTaskContext(context);
   }

   public Optional<String> getRole() {
      return task.getAllowedRole();
   }
}
