package org.infinispan.server.tasks;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.server.logging.Log;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.TaskInstantiationMode;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public class ServerTaskWrapper<T> implements Task, Function<TaskContext, T> {
   private static final Log log = LogFactory.getLog(ServerTaskWrapper.class, Log.class);
   private final ServerTask<T> task;

   public ServerTaskWrapper(ServerTask<T> task) {
      this.task = task;
   }

   @Override
   public String getName() {
      return task.getName();
   }

   public T run(TaskContext context) throws Exception {
      final ServerTask<T> t;
      if (task.getInstantiationMode() == TaskInstantiationMode.ISOLATED) {
         t = Util.getInstance(task.getClass());
      } else {
         t = task;
      }
      t.setTaskContext(context);
      if (log.isTraceEnabled()) {
         log.tracef("Executing task '%s' in '%s' mode using context %s", getName(), getInstantiationMode(), context);
      }
      return t.call();
   }

   @Override
   public T apply(TaskContext context) {
      try {
         return run(context);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public String getType() {
      return "Java";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return task.getExecutionMode();
   }

   @Override
   public TaskInstantiationMode getInstantiationMode() {
      return task.getInstantiationMode();
   }

   public Optional<String> getRole() {
      return task.getAllowedRole();
   }

   @Override
   public Set<String> getParameters() {
      return task.getParameters();
   }
}
