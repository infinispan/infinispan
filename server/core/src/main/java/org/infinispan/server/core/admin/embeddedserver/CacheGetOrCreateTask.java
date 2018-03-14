package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.Map;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Admin operation to create a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to create</li>
 *    <li><strong>template</strong> the name of the template to use</li>
 *    <li><strong>configuration</strong> the XML configuration to use</li>
 *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
public class CacheGetOrCreateTask extends CacheCreateTask {
   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "getorcreate";
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters, "name");
      String template = getParameter(parameters, "template");
      String configuration = getParameter(parameters, "configuration");
      if (configuration != null) {
         Configuration config = getConfiguration(name, configuration);
         cacheManager.administration().withFlags(flags).getOrCreateCache(name, config);
      } else {
         cacheManager.administration().withFlags(flags).getOrCreateCache(name, template);
      }
      return null;
   }


}
