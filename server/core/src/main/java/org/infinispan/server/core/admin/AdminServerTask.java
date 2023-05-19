package org.infinispan.server.core.admin;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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
      Map<String, List<String>> parameters = raw.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> {
               Object value = entry.getValue();
               if (value instanceof String) {
                  return Collections.singletonList((String) value);
               } else if (value instanceof String[]) {
                  return Arrays.asList((String[]) value);
               } else if (value instanceof List) {
                  return (List)value;
               } else if (value instanceof byte[]) {
                  return Collections.singletonList(new String((byte[]) value, StandardCharsets.UTF_8));
               } else {
                  throw log.illegalParameterType(entry.getKey(), value.getClass());
               }
            }));
      List<String> sFlags = parameters.remove("flags");
      return execute(
            taskContext.getCacheManager(),
            parameters,
            sFlags != null ? AdminFlag.fromString(sFlags.get(0)) : EnumSet.noneOf(AdminFlag.class)
      );
   }

   protected String requireParameter(Map<String, List<String>> parameters, String parameter) {
      List<String> v = parameters.get(parameter);
      if (v == null) {
         throw log.missingRequiredAdminTaskParameter(getName(), parameter);
      } else {
         return v.get(0);
      }
   }

   protected String getParameter(Map<String, List<String>> parameters, String parameter) {
      List<String> v = parameters.get(parameter);
      return v == null ? null : v.get(0);
   }

   protected abstract T execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<AdminFlag> adminFlags);

   public abstract String getTaskContextName();

   public abstract String getTaskOperationName();
}
