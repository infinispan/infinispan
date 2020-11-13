package org.infinispan.rest.resources;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskManager;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

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
      // Register our custom serializers
      SimpleModule module = new SimpleModule();
      module.addSerializer(LoggerConfig.class, new Log4j2LoggerConfigSerializer());
      module.addSerializer(Appender.class, new Log4j2AppenderSerializer());
      this.invocationHelper.getMapper().registerModule(module);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
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
         return completedFuture(new NettyRestResponse.Builder().status(HttpResponseStatus.BAD_REQUEST).build());
      }
      return taskManager.runTask("@@logging@set",
            new TaskContext()
                  .addOptionalParameter("loggerName", loggerName)
                  .addOptionalParameter("level", level)
                  .addOptionalParameter("appenders", appenders)
                  .subject(request.getSubject())
      ).handle((o, t) -> handle(t));
   }

   private CompletionStage<RestResponse> deleteLogger(RestRequest request) {
      TaskManager taskManager = invocationHelper.getServer().getTaskManager();
      String loggerName = request.variables().get("loggerName");
      return taskManager.runTask("@@logging@remove",
            new TaskContext()
                  .addParameter("loggerName", loggerName)
                  .subject(request.getSubject())
            ).handle((o, t) -> handle(t));
   }

   private CompletionStage<RestResponse> listLoggers(RestRequest request) {
      // We only return loggers declared in the configuration
      LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
      return asJsonResponseFuture(logContext.getConfiguration().getLoggers().values(), invocationHelper);
   }

   private CompletionStage<RestResponse> listAppenders(RestRequest request) {
      LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
      return asJsonResponseFuture(logContext.getConfiguration().getAppenders(), invocationHelper);
   }

   public static class Log4j2LoggerConfigSerializer extends StdSerializer<LoggerConfig> {

      public Log4j2LoggerConfigSerializer() {
         this(null);
      }

      public Log4j2LoggerConfigSerializer(Class<LoggerConfig> t) {
         super(t);
      }

      @Override
      public void serialize(LoggerConfig logger, JsonGenerator json, SerializerProvider serializerProvider) throws IOException {
         json.writeStartObject();
         json.writeStringField("name", logger.getName());
         json.writeStringField("level", logger.getLevel().name());
         json.writeArrayFieldStart("appenders");
         for (String appender : logger.getAppenders().keySet()) {
            json.writeString(appender);
         }
         json.writeEndArray();
         json.writeEndObject();
      }
   }

   public static class Log4j2AppenderSerializer extends StdSerializer<Appender> {

      public Log4j2AppenderSerializer() {
         this(null);
      }

      public Log4j2AppenderSerializer(Class<Appender> t) {
         super(t);
      }

      @Override
      public void serialize(Appender appender, JsonGenerator json, SerializerProvider serializerProvider) throws IOException {
         json.writeStartObject();
         json.writeStringField("name", appender.getName());
         json.writeEndObject();
      }
   }

   private NettyRestResponse handle(Throwable t) {
      NettyRestResponse.Builder response = new NettyRestResponse.Builder();
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
