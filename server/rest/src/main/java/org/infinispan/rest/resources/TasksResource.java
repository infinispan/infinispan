package org.infinispan.rest.resources;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_JAVASCRIPT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.manager.TaskManager;

/**
 * @since 10.1
 */
public class TasksResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;

   public TasksResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("tasks", "REST endpoint to manage tasks.")
            .invocation().methods(GET).path("/v2/tasks/").handleWith(this::listTasks)
            .invocation().methods(PUT, POST).path("/v2/tasks/{taskName}").handleWith(this::createScriptTask)
            .invocation().methods(POST).path("/v2/tasks/{taskName}").withAction("exec").handleWith(this::runTask)
            .invocation().methods(GET).path("/v2/tasks/{taskName}").withAction("script").handleWith(this::getScript)
            .create();
   }

   private CompletionStage<RestResponse> getScript(RestRequest request) {
      String taskName = request.variables().get("taskName");
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance().withSubject(request.getSubject());
      ScriptingManager scriptingManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(ScriptingManager.class);
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      if (!scriptingManager.getScriptNames().contains(taskName)) {
         throw Log.REST.noSuchScript(taskName);
      }

      return CompletableFuture.supplyAsync(() -> {
         String script = Security.doAs(request.getSubject(), () -> scriptingManager.getScript(taskName));
         return builder.entity(script).contentType(TEXT_PLAIN_TYPE).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> listTasks(RestRequest request) {
      String type = request.getParameter("type");
      boolean userOnly = "user".equalsIgnoreCase(type);
      boolean pretty = isPretty(request);
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      TaskManager taskManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(TaskManager.class);

      return (userOnly ? taskManager.getUserTasksAsync() : taskManager.getTasksAsync())
            .thenApply(tasks -> asJsonResponse(invocationHelper.newResponse(request), Json.make(tasks), pretty));
   }

   private CompletionStage<RestResponse> createScriptTask(RestRequest request) {
      String taskName = request.variables().get("taskName");
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance().withSubject(request.getSubject());
      ScriptingManager scriptingManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(ScriptingManager.class);
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      ContentSource contents = request.contents();
      byte[] bytes = contents.rawContent();
      MediaType sourceType = request.contentType() == null ? TEXT_JAVASCRIPT : request.contentType();
      String script = StandardConversions.convertTextToObject(bytes, sourceType);

      return CompletableFuture.supplyAsync(() -> {
         Security.doAs(request.getSubject(), () -> scriptingManager.addScript(taskName, script));
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> runTask(RestRequest request) {
      String taskName = request.variables().get("taskName");
      boolean pretty = isPretty(request);
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      TaskManager taskManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(TaskManager.class);
      TaskContext taskContext = new TaskContext();
      request.parameters().forEach((k, v) -> {
         if ("cache".equals(k)) {
            AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(v.get(0), request);
            taskContext.cache(cache);
         } else if (k.startsWith("param.")) {
            taskContext.addParameter(k.substring(6), v.get(0));
         }
      });

      CompletionStage<Object> runResult = Security.doAs(request.getSubject(), () -> taskManager.runTask(taskName, taskContext));

      return runResult.thenApply(result -> {
         NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
         if (result instanceof byte[]) {
            builder.contentType(TEXT_PLAIN_TYPE).entity(result);
         } else {
            addEntityAsJson(Json.make(result), builder, pretty);
         }
         return builder.build();
      });
   }
}
