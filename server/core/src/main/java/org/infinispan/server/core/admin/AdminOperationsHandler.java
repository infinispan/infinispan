package org.infinispan.server.core.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheException;
import org.infinispan.security.Security;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * AdminOperationsHandler is a special {@link TaskEngine} which can handle admin tasks
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public abstract class AdminOperationsHandler implements TaskEngine {
   final Map<String, AdminServerTask> tasks;

   protected AdminOperationsHandler(AdminServerTask<?>... tasks) {
      this.tasks = new HashMap<>(tasks.length);
      for (AdminServerTask<?> task : tasks) {
         this.tasks.put(task.getName(), task);
      }
   }

   @Override
   public String getName() {
      return this.getClass().getSimpleName();
   }

   @Override
   public List<Task> getTasks() {
      return new ArrayList(tasks.values());
   }

   @Override
   public <T> CompletionStage<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager) {
      AdminServerTask<T> task = tasks.get(taskName);
      Subject subject = context.getSubject().orElse(Security.getSubject());
      return blockingManager.supplyBlocking(() -> {
         try {
            return Security.doAs(subject, () -> task.execute(context));
         } catch (CacheException e) {
            throw e;
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }, taskName);
   }

   @Override
   public boolean handles(String taskName) {
      return tasks.containsKey(taskName);
   }
}
