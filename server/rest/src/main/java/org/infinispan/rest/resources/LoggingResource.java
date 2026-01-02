package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.manager.TaskManager;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Logging configuration resource. This resource is log4j 2.x-specific
 *
 * @author tristan@infinispan.org
 * @since 11.0
 */
public final class LoggingResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;

   public LoggingResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("logging", "REST resource to manage logging.")
            .invocation().methods(GET).path("/v2/logging/loggers").handleWith(this::listLoggers)
            .invocation().methods(GET).path("/v2/logging/appenders").handleWith(this::listAppenders)
            .invocation().methods(DELETE).path("/v2/logging/loggers/{loggerName}").handleWith(this::deleteLogger)
            .invocation().methods(PUT).path("/v2/logging/loggers/{loggerName}").handleWith(this::setLogger)
            .invocation().methods(PUT).path("/v2/logging/loggers").handleWith(this::setLogger)
            .create();
   }

   private CompletionStage<RestResponse> setLogger(RestRequest request) {
      TaskManager taskManager = invocationHelper.getServer().getTaskManager();
      String loggerName = request.variables().get("loggerName");
      String level = request.getParameter("level");
      List<String> appenders = request.parameters().get("appender");
      if (level == null && appenders == null) {
         throw Log.REST.missingArguments("level", "appender");
      }
      return taskManager.runTask("@@logging@set",
            new TaskContext()
                  .addOptionalParameter("loggerName", loggerName)
                  .addOptionalParameter("level", level)
                  .addOptionalParameter("appenders", appenders)
                  .subject(request.getSubject())
      ).handle((o, t) -> handle(request, t));
   }

   private CompletionStage<RestResponse> deleteLogger(RestRequest request) {
      TaskManager taskManager = invocationHelper.getServer().getTaskManager();
      String loggerName = request.variables().get("loggerName");
      return taskManager.runTask("@@logging@remove",
            new TaskContext()
                  .addParameter("loggerName", loggerName)
                  .subject(request.getSubject())
      ).handle((o, t) -> handle(request, t));
   }

   private CompletionStage<RestResponse> listLoggers(RestRequest request) {
      // We only return loggers declared in the configuration
      LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
      Json loggerConfigs = jsonLoggerConfigs(logContext.getConfiguration().getLoggers().values());
      return asJsonResponseFuture(invocationHelper.newResponse(request), loggerConfigs, isPretty(request));
   }

   private Json jsonLoggerConfigs(Collection<LoggerConfig> loggerConfigs) {
      Json array = Json.array();
      for (LoggerConfig loggerConfig : loggerConfigs) {
         Json jsonLoggerConfig = Json.object();
         jsonLoggerConfig.set("name", loggerConfig.getName());
         jsonLoggerConfig.set("level", loggerConfig.getLevel().toString());
         jsonLoggerConfig.set("appenders", Json.make(loggerConfig.getAppenders().keySet()));
         array.add(jsonLoggerConfig);
      }
      return array;
   }

   private CompletionStage<RestResponse> listAppenders(RestRequest request) {
      LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
      Map<String, Appender> appendersMap = logContext.getConfiguration().getAppenders();
      Json jsonMap = Json.object();
      appendersMap.forEach((key, value) -> jsonMap.set(key, Json.object().set("name", value.getName())));
      return asJsonResponseFuture(invocationHelper.newResponse(request), jsonMap, isPretty(request));
   }

   private NettyRestResponse handle(RestRequest request, Throwable t) {
      NettyRestResponse.Builder response = invocationHelper.newResponse(request);
      if (t == null) {
         response.status(HttpResponseStatus.NO_CONTENT);
      } else {
         while (t.getCause() != null) {
            t = t.getCause();
         }
         if (t instanceof IllegalStateException) {
            response.status(HttpResponseStatus.CONFLICT).entity(t.getMessage());
         } else if (t instanceof IllegalArgumentException) {
            response.status(HttpResponseStatus.BAD_REQUEST).entity(t.getMessage());
         } else if (t instanceof NoSuchElementException) {
            response.status(HttpResponseStatus.NOT_FOUND).entity(t.getMessage());
         } else if (t instanceof SecurityException) {
            response.status(HttpResponseStatus.FORBIDDEN).entity(t.getMessage());
         } else {
            response.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(t.getMessage());
         }
      }
      return response.build();
   }
}
