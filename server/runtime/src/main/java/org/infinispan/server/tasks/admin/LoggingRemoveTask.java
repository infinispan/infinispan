package org.infinispan.server.tasks.admin;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.core.admin.AdminServerTask;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * Admin operation to remove a logger
 *
 * @author Tristan Tarrant
 * @since 11.0
 */
public class LoggingRemoveTask extends AdminServerTask<byte[]> {
   @Override
   public String getTaskContextName() {
      return "logging";
   }

   @Override
   public String getTaskOperationName() {
      return "remove";
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }

   @Override
   public Set<String> getParameters() {
      return Collections.singleton("loggerName");
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      String loggerName = requireParameter(parameters, "loggerName");
      LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
      Configuration configuration = logContext.getConfiguration();
      if (configuration.getLoggers().containsKey(loggerName)) {
         configuration.removeLogger(loggerName);
         logContext.updateLoggers();
      } else {
         throw new NoSuchElementException(loggerName);
      }
      return null;
   }
}
