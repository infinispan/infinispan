package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminFlag;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to create a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to remove</li>
 *    <li><strong>flags</strong> </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheCreateTask extends AdminServerTask<Void> {
   private static Set<String> PARAMETERS;

   static {
      PARAMETERS = new HashSet<>();
      PARAMETERS.add("name");
      PARAMETERS.add("template");
   }

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "create";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<AdminFlag> flags) {
      if (isPersistent(flags))
         throw new UnsupportedOperationException();

      String name = requireParameter(parameters,"name");
      String template = getParameter(parameters, "template");
      cacheManager.executor().submitConsumer(localManager -> {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (template != null) {
            builder.read(localManager.getCacheConfiguration(template));
         } else {
            Configuration parent = localManager.getDefaultCacheConfiguration();
            if (parent != null)
               builder.read(parent);
         }
         builder.template(false);
         localManager.defineConfiguration(name, builder.build());
         localManager.getCache(name);
         return null;
      }, (address, value, throwable) -> {
         if (throwable != null) {
            log.fatal("Cache startup encountered exception on node " + address, throwable);
         }
      }).join();

      return null;
   }
}
