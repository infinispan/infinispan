package org.infinispan.server.core.admin;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.logging.LogFactory;

/**
 * Common base for admin server tasks
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public abstract class AdminServerTask<T> implements Task {
   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Override
   public final String getName() {
      return "@@" + getTaskContextName() + "@" + getTaskOperationName();
   }

   @Override
   public String getType() {
      return AdminServerTask.class.getSimpleName();
   }

   public final T execute(TaskContext taskContext) {
      Map<String, ?> raw = taskContext.getParameters().orElse(Collections.emptyMap());
      Map<String, String> parameters = raw.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> {
               Object value = entry.getValue();
               if (value instanceof String) {
                  return (String) value;
               } else if (value instanceof byte[]) {
                  return new String((byte[]) value, StandardCharsets.UTF_8);
               } else {
                  throw log.illegalParameterType(entry.getKey(), value.getClass());
               }
            }));
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
