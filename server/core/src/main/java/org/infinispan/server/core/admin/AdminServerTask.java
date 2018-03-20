package org.infinispan.server.core.admin;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.logging.LogFactory;

import io.netty.util.CharsetUtil;

/**
 * Common base for admin server tasks
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public abstract class AdminServerTask<T> implements Task {
   protected final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Override
   public final String getName() {
      return "@@" + getTaskContextName() + "@" + getTaskOperationName();
   }

   @Override
   public String getType() {
      return AdminServerTask.class.getSimpleName();
   }

   public final T execute(TaskContext taskContext) {
      Map<String, byte[]> rawParams = (Map<String, byte[]>) taskContext.getParameters().get();
      Map<String, String> parameters = rawParams.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String(entry.getValue(), CharsetUtil.UTF_8)));
      String sFlags = parameters.remove("flags");
      T result = execute(
            taskContext.getCacheManager(),
            parameters,
            AdminFlag.fromString(sFlags)
      );
      return result;
   }

   protected String requireParameter(Map<String, String> parameters, String parameter) {
      String v = parameters.get(parameter);
      if (v == null) {
         throw log.missingRequiredAdminTaskParameter(getName(), parameter);
      } else {
         return v;
      }
   }

   protected String getParameter(Map<String, String> parameters, String parameter) {
      return parameters.get(parameter);
   }

   protected abstract T execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<AdminFlag> adminFlags);

   public abstract String getTaskContextName();

   public abstract String getTaskOperationName();
}
