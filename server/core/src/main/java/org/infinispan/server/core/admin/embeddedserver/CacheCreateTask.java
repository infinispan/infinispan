package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to create a cache
 * Parameters:
 * <ul>
 *    <li><b>name</b> the name of the cache to create</li>
 *    <li><b>flags</b> any flags, e.g. PERMANENT</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheCreateTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS = Set.of("name", "template", "configuration");

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
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters, "name");
      String template = getParameter(parameters, "template");
      String configuration = getParameter(parameters, "configuration");
      if (configuration != null) {
         Configuration config = getConfigurationBuilder(name, configuration).build();
         if (!cacheManager.getCacheManagerConfiguration().isClustered() && config.clustering().cacheMode().isClustered()) {
            throw log.cannotCreateClusteredCache();
         }
         cacheManager.administration().withFlags(flags).createCache(name, config);
      } else {
         cacheManager.administration().withFlags(flags).createCache(name, template);
      }
      return null;
   }

   protected ConfigurationBuilder getConfigurationBuilder(String name, String configuration) {
      ParserRegistry parser = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parser.parse(configuration, null); // Use type auto-detection
      Map<String, ConfigurationBuilder> builders = builderHolder.getNamedConfigurationBuilders();
      if (builders.isEmpty()) {
         throw log.missingCacheConfiguration(name, configuration);
      } else if (builders.size() > 1) {
         throw log.configurationMustContainSingleCache(name, configuration);
      }
      return builders.values().iterator().next();
   }
}
