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
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @author Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 */
public class ServerTaskEngine implements TaskEngine {
   private final LocalServerTaskRunner localRunner;
   private final DistributedServerTaskRunner distributedRunner;

   private final Authorizer globalauthorizer;
   private final ScriptConversions scriptConversions;
   private final Map<String, ServerTaskWrapper> tasks;

   public ServerTaskEngine(EmbeddedCacheManager cacheManager, Map<String, ServerTaskWrapper> tasks) {
      GlobalComponentRegistry registry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      registry.registerComponent(this, ServerTaskEngine.class);
      SerializationContextRegistry serializationContextRegistry = registry.getComponent(SerializationContextRegistry.class);
      serializationContextRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());
      EncoderRegistry encoderRegistry = registry.getComponent(EncoderRegistry.class);
      this.scriptConversions = new ScriptConversions(encoderRegistry);
      this.globalauthorizer = registry.getComponent(Authorizer.class);
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
      if (task.getExecutionMode() == TaskExecutionMode.ONE_NODE) {
         runner = localRunner;
      } else {
         runner = distributedRunner;
      }
      launderParameters(context);
      MediaType requestMediaType = context.getCache().map(c -> c.getAdvancedCache().getValueDataConversion().getRequestMediaType()).orElse(MediaType.MATCH_ALL);
      context.getCache().ifPresent(c -> context.cache(c.getAdvancedCache().withMediaType(APPLICATION_OBJECT, APPLICATION_OBJECT)));
      return runner.execute(task.getName(), context).thenApply(r -> (T) scriptConversions.convertToRequestType(r, APPLICATION_OBJECT, requestMediaType));
   }

   private void launderParameters(TaskContext context) {
      if (context.getParameters().isPresent()) {
         Map<String, Object> convertParameters = scriptConversions.convertParameters(context);
         context.parameters(convertParameters);
      }
   }

   private <T> void checkPermissions(TaskContext context, ServerTaskWrapper<T> task) {
      String role = task.getRole().orElse(null);
      if (globalauthorizer != null) {
         if (context.getCache().isPresent()) {
            AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(context.getCache().get().getAdvancedCache());
            if (authorizationManager != null) {
               // when the cache is secured
               authorizationManager.checkPermission(AuthorizationPermission.EXEC, role);
               return;
            }
         }
         if (context.getSubject().isPresent()) {
            // if the subject is present, then use the subject
            globalauthorizer.checkPermission(context.getSubject().get(), AuthorizationPermission.EXEC);
            return;
         }
         globalauthorizer.checkPermission(null, null, AuthorizationPermission.EXEC, role);
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
