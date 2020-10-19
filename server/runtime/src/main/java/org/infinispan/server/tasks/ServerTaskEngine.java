package org.infinispan.server.tasks;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.scripting.utils.ScriptConversions;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public class ServerTaskEngine implements TaskEngine {
   private final LocalServerTaskRunner localRunner;
   private final DistributedServerTaskRunner distributedRunner;

   private final AuthorizationHelper globalAuthzHelper;
   private final ScriptConversions scriptConversions;
   private final Map<String, ServerTaskWrapper> tasks;

   public ServerTaskEngine(EmbeddedCacheManager cacheManager, Map<String, ServerTaskWrapper> tasks) {
      GlobalComponentRegistry registry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      registry.registerComponent(this, ServerTaskEngine.class);
      SerializationContextRegistry serializationContextRegistry = registry.getComponent(SerializationContextRegistry.class);
      serializationContextRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());
      EncoderRegistry encoderRegistry = registry.getComponent(EncoderRegistry.class);
      this.scriptConversions = new ScriptConversions(encoderRegistry);
      this.globalAuthzHelper = registry.getComponent(AuthorizationHelper.class);
      this.tasks = tasks;
      this.localRunner = new LocalServerTaskRunner(this);
      this.distributedRunner = new DistributedServerTaskRunner();
   }

   @Override
   public String getName() {
      return "Deployed";
   }

   @Override
   public List<Task> getTasks() {
      return new ArrayList<>(tasks.values());
   }

   @Override
   public <T> CompletableFuture<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager) {
      ServerTaskWrapper<T> task = tasks.get(taskName);
      if (task == null) {
         throw new IllegalArgumentException("Task not found: " + taskName);
      }
      checkPermissions(context, task);
      return invokeTask(context, task);
   }

   private <T> CompletableFuture<T> invokeTask(TaskContext context, ServerTaskWrapper<T> task) {
      ServerTaskRunner runner;
      switch (task.getExecutionMode()) {
         case ONE_NODE:
            runner = localRunner;
            break;
         default:
            runner = distributedRunner;
            break;
      }
      launderParameters(context);
      MediaType requestMediaType = context.getCache().map(c -> c.getAdvancedCache().getValueDataConversion().getRequestMediaType()).orElse(MediaType.MATCH_ALL);
      context.getCache().ifPresent(c -> context.cache(c.getAdvancedCache().withMediaType(APPLICATION_OBJECT, APPLICATION_OBJECT)));
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
      return tasks.containsKey(taskName);
   }

   public <T> ServerTaskWrapper<T> getTask(String taskName) {
      return tasks.get(taskName);
   }
}
