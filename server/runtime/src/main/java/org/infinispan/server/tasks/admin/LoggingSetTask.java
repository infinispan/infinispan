package org.infinispan.server.tasks.admin;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.Server;
import org.infinispan.server.core.admin.AdminServerTask;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * Admin operation to add/modify a logger
 *
 * @author Tristan Tarrant
 * @since 11.0
 */
public class LoggingSetTask extends AdminServerTask<byte[]> {
   private static final Set<String> PARAMETERS = Stream.of("loggerName", "level", "appenders", "create").collect(Collectors.toSet());

   @Override
   public String getTaskContextName() {
      return "logging";
   }

   @Override
   public String getTaskOperationName() {
      return "set";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      String loggerName = getParameter(parameters, "loggerName");
      List<String> appenders = parameters.get("appenders");
      LoggerContext context = (LoggerContext) LogManager.getContext(false);

      String pLevel = getParameter(parameters, "level");
      Level level;
      if (pLevel == null) {
         level = null;
      } else {
         level = Level.getLevel(pLevel);
         if (level == null) {
            throw Server.log.invalidLevel(pLevel);
         }
      }

      Configuration configuration = context.getConfiguration();
      LoggerConfig loggerConfig = loggerName == null ? configuration.getRootLogger() : configuration.getLoggerConfig(loggerName);
      if (level == null) {
         // If no level was specified use the existing one or the root one
         level = loggerConfig.getLevel();
      }
      LoggerConfig specificConfig = loggerConfig;
      if (loggerName != null && !loggerConfig.getName().equals(loggerName)) {
         specificConfig = new LoggerConfig(loggerName, level, true);
         specificConfig.setParent(loggerConfig);
         configuration.addLogger(loggerName, specificConfig);
      }

      if (appenders != null) {
         Map<String, Appender> rootAppenders = context.getRootLogger().getAppenders();
         Map<String, Appender> loggerAppenders = specificConfig.getAppenders();
         // Remove all unwanted appenders
         loggerAppenders.keySet().removeAll(appenders);
         for (String appender : loggerAppenders.keySet()) {
            specificConfig.removeAppender(appender);
         }
         // Add any missing appenders
         for (String appender : appenders) {
            Appender app = rootAppenders.get(appender);
            if (app != null) {
               specificConfig.addAppender(app, level, null);
            } else {
               throw Server.log.unknownAppender(appender);
            }
         }
      }

      specificConfig.setLevel(level);
      context.updateLoggers();
      return null;
   }
}
