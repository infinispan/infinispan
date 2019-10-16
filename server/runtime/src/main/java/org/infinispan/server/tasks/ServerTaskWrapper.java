package org.infinispan.server.tasks;

import java.util.Optional;
import java.util.Set;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
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

   @Override
   public Set<String> getParameters() {
      return task.getParameters();
   }


}
