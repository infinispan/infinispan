package org.infinispan.server.core.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.spi.TaskEngine;

/**
 * AdminOperationsHandler is a special {@link TaskEngine} which can handle admin tasks
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public abstract class AdminOperationsHandler implements TaskEngine {
   final Map<String, AdminServerTask> tasks;

   protected AdminOperationsHandler(Class<? extends AdminServerTask>... taskClasses) {
      this.tasks = new HashMap<>(taskClasses.length);
      for(Class<? extends AdminServerTask> taskClass : taskClasses) {
         AdminServerTask<?> task = Util.getInstance(taskClass);
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
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context, Executor executor) {
      AdminServerTask<T> task = tasks.get(taskName);
      return CompletableFuture.supplyAsync(() -> {
         try {
            return task.execute(context);
         } catch (CacheException e) {
            throw e;
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }, executor);
   }

   @Override
   public boolean handles(String taskName) {
      return tasks.containsKey(taskName);
   }
}
