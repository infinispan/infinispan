package org.infinispan.rest.resources;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JAVASCRIPT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

import javax.security.auth.Subject;

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
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/tasks/").handleWith(this::listTasks)
            .invocation().methods(PUT, POST).path("/v2/tasks/{taskName}").handleWith(this::createScriptTask)
            .invocation().methods(GET).path("/v2/tasks/{taskName}").withAction("exec").handleWith(this::runTask)
            .create();
   }

   private CompletionStage<RestResponse> listTasks(RestRequest request) {
      String type = request.getParameter("type");
      boolean userOnly = type != null && type.equalsIgnoreCase("user");

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      TaskManager taskManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(TaskManager.class);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      try {
         List<Task> tasks = userOnly ? taskManager.getUserTasks() : taskManager.getTasks();
         byte[] resultBytes = invocationHelper.getMapper().writeValueAsBytes(tasks);
         builder.contentType(APPLICATION_JSON_TYPE).entity(resultBytes);
      } catch (JsonProcessingException e) {
         builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage());
      }
      return completedFuture(builder.build());
   }

   private CompletionStage<RestResponse> createScriptTask(RestRequest request) {
      String taskName = request.variables().get("taskName");
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance().withSubject(request.getSubject());
      ScriptingManager scriptingManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(ScriptingManager.class);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      ContentSource contents = request.contents();
      byte[] bytes = contents.rawContent();
      MediaType sourceType = request.contentType() == null ? APPLICATION_JAVASCRIPT : request.contentType();
      String script = StandardConversions.convertTextToObject(bytes, sourceType);
      Subject.doAs(request.getSubject(), (PrivilegedAction<Void>) () -> {
         scriptingManager.addScript(taskName, script);
         return null;
      });
      return completedFuture(builder.build());
   }

   private CompletionStage<RestResponse> runTask(RestRequest request) {
      String taskName = request.variables().get("taskName");
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      TaskManager taskManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(TaskManager.class);
      TaskContext taskContext = new TaskContext();
      request.parameters().forEach((k, v) -> {
         if (k.startsWith("param.")) {
            taskContext.addParameter(k.substring(6), v.get(0));
         }
      });

      CompletionStage<Object> runResult = Subject.doAs(request.getSubject(),
            (PrivilegedAction<CompletionStage<Object>>) () -> taskManager.runTask(taskName, taskContext));

      return runResult.thenApply(result -> {
         NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
         try {
            if (result instanceof byte[]) {
               builder.contentType(TEXT_PLAIN_TYPE).entity(result);
            } else {
               byte[] resultBytes = invocationHelper.getMapper().writeValueAsBytes(result);
               builder.contentType(APPLICATION_JSON_TYPE).entity(resultBytes);
            }
         } catch (JsonProcessingException e) {
            builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage());
         }
         return builder.build();
      });
   }
}
