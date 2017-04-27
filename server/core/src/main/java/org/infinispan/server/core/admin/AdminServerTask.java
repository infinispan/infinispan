package org.infinispan.server.core.admin;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.util.logging.LogFactory;

import io.netty.util.CharsetUtil;

/**
 * Common base for admin server tasks
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public abstract class AdminServerTask<T> implements ServerTask<T> {
   protected EmbeddedCacheManager cacheManager;
   protected final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private Map<String, String> parameters;
   private EnumSet<AdminFlag> flags;

   @Override
   public final String getName() {
      return "@@" + getTaskContextName() + "@" + getTaskOperationName();
   }

   @Override
   public final void setTaskContext(TaskContext taskContext) {
      this.cacheManager = taskContext.getCacheManager();
      Map<String, byte[]> rawParams = (Map<String, byte[]>) taskContext.getParameters().get();
      parameters = rawParams.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String(entry.getValue(), CharsetUtil.UTF_8)));
      String sFlags = this.parameters.remove("flags");
      flags = AdminFlag.fromString(sFlags);
   }

   public boolean isPersistent() {
      return flags.contains(AdminFlag.PERSISTENT);
   }

   protected String requireParameter(String parameter) {
      String v = parameters.get(parameter);
      if (v == null) {
         throw log.missingRequiredAdminTaskParameter(getName(), parameter);
      } else {
         return v;
      }
   }

   protected String getParameter(String parameter) {
      return parameters.get(parameter);
   }

   public abstract String getTaskContextName();

   public abstract String getTaskOperationName();
}
