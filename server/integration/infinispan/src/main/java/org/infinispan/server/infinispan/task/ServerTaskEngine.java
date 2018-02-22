package org.infinispan.server.infinispan.task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.spi.TaskEngine;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 12:32 PM
 */
public class ServerTaskEngine implements TaskEngine {
   private final AuthorizationHelper globalAuthzHelper;
   private final ServerTaskRegistry registry;
   private final ServerTaskRunnerFactory runnerFactory = new ServerTaskRunnerFactory();

   public ServerTaskEngine(ServerTaskRegistry manager, EmbeddedCacheManager cacheManager) {
      this.registry = manager;
      this.globalAuthzHelper = cacheManager.getGlobalComponentRegistry().getComponent(AuthorizationHelper.class);
   }

   @Override
   public String getName() {
      return "Deployed";
   }

   @Override
   public List<Task> getTasks() {
      return registry.getTasks();
   }

   @Override
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context, Executor executor) {
      ServerTaskWrapper<T> task = registry.getTask(taskName);
      if (task == null) {
         throw new IllegalArgumentException("Task not found: " + taskName);
      }
      checkPermissions(context, task);
      return invokeTask(context, task);
   }

   private <T> CompletableFuture<T> invokeTask(TaskContext context, ServerTaskWrapper<T> task) {
      ServerTaskRunner runner = runnerFactory.getRunner(task.getExecutionMode());
      launderParameters(context);
      return runner.execute(task.getName(), context).thenApply(r ->
            (T) context.getMarshaller().map(m -> toBytes(r, m)).orElse(r));
   }

   private static Object toBytes(Object obj, Marshaller marshaller) {
      try {
         return marshaller.objectToByteBuffer(obj);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private void launderParameters(TaskContext context) {
      if (context.getParameters().isPresent() && context.getMarshaller().isPresent()) {
         Map<String, ?> params = context.getParameters().get();
         Marshaller m = context.getMarshaller().get();
         for (Map.Entry<String, ?> e : params.entrySet()) {
            Object v = e.getValue();
            Object entryValue = v instanceof byte[] ? fromBytes(v, m) : v;
            context.addParameter(e.getKey(), entryValue);
         }
      }
   }

   private static Object fromBytes(Object obj, Marshaller marshaller) {
      try {
         return marshaller.objectFromByteBuffer((byte[]) obj);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private <T> void checkPermissions(TaskContext context, ServerTaskWrapper<T> task) {
      String role = task.getRole().orElse(null);
      if (globalAuthzHelper != null) {
         if (context.getCache().isPresent()) {
            AuthorizationManager authorizationManager = context.getCache().get().getAdvancedCache().getAuthorizationManager();
            if (authorizationManager != null) {
               authorizationManager.checkPermission(AuthorizationPermission.EXEC, role);
               return;
            }
         }
         globalAuthzHelper.checkPermission(null, null, AuthorizationPermission.EXEC, role);
      }
   }

   @Override
   public boolean handles(String taskName) {
      return registry.handles(taskName);
   }
}
