package org.infinispan.server.infinispan.task;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.utils.ScriptConversions;
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
   private final ScriptConversions scriptConversions;
   private final ServerTaskRegistry registry;
   private final ServerTaskRunnerFactory runnerFactory = new ServerTaskRunnerFactory();

   public ServerTaskEngine(ServerTaskRegistry manager, EmbeddedCacheManager cacheManager, ScriptConversions scriptConversions) {
      this.registry = manager;
      this.globalAuthzHelper = cacheManager.getGlobalComponentRegistry().getComponent(AuthorizationHelper.class);
      this.scriptConversions = scriptConversions;
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
      MediaType requestMediaType = context.getCache().map(c -> c.getAdvancedCache().getValueDataConversion().getRequestMediaType()).orElse(MediaType.MATCH_ALL);
      context.getCache().ifPresent(c -> context.cache(c.getAdvancedCache().withMediaType(APPLICATION_OBJECT_TYPE, APPLICATION_OBJECT_TYPE)));
      return runner.execute(task.getName(), context).thenApply(r -> (T) scriptConversions.convertToRequestType(r, APPLICATION_OBJECT, requestMediaType));
   }

   private void launderParameters(TaskContext context) {
      if (context.getParameters().isPresent()) {
         Map<String, ?> convertParameters = scriptConversions.convertParameters(context);
         context.parameters(convertParameters);
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
