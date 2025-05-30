package org.infinispan.server.tasks.admin;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Admin operation to create a cache
 * Parameters:
 * <ul>
 *    <li><b>name</b> the name of the cache to create</li>
 *    <li><b>template</b> the name of the template to use</li>
 *    <li><b>configuration</b> the XML configuration to use</li>
 *    <li><b>flags</b> any flags, e.g. PERMANENT</li>
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
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters, "name");
      String template = getParameter(parameters, "template");
      String configuration = getParameter(parameters, "configuration");

      if (!cacheManager.cacheExists(name) && !cacheManager.getStatus().allowInvocations())
         throw log.cacheIsNotReady(name);

      if (configuration != null) {
         Configuration config = getConfigurationBuilder(name, configuration).build();
         cacheManager.administration().withFlags(flags).getOrCreateCache(name, config);
      } else {
         cacheManager.administration().withFlags(flags).getOrCreateCache(name, template);
      }
      return null;
   }


}
