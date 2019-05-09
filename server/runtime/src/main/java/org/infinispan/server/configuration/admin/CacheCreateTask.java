package org.infinispan.server.configuration.admin;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;

/**
 * Admin operation to create a cache. This
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to create</li>
 *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class CacheCreateTask extends org.infinispan.server.core.admin.embeddedserver.CacheCreateTask {
   final  protected ConfigurationBuilderHolder defaultsHolder;

   public CacheCreateTask(ConfigurationBuilderHolder defaultsHolder) {
      this.defaultsHolder = defaultsHolder;
   }

   protected Configuration getConfiguration(String name, String configuration) {
      ParserRegistry parser = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parser.parse(configuration);
      if (!builderHolder.getNamedConfigurationBuilders().containsKey(name)) {
         throw log.missingCacheConfiguration(name, configuration);
      }
      // Rebase the configuration on top of the defaults
      Configuration cfg = builderHolder.getNamedConfigurationBuilders().get(name).build();
      ConfigurationBuilder defaultCfg = defaultsHolder.getNamedConfigurationBuilders().get(cfg.clustering().cacheMode().name());
      ConfigurationBuilder rebased = new ConfigurationBuilder().read(defaultCfg.build());
      rebased.read(cfg);
      return rebased.build();
   }
}
