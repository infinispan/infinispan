package org.infinispan.server.tasks.admin;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;

/**
 * Admin operation to create a cache. This Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to create</li>
 *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class CacheCreateTask extends org.infinispan.server.core.admin.embeddedserver.CacheCreateTask {
   final protected ConfigurationBuilderHolder defaultsHolder;

   public CacheCreateTask(ConfigurationBuilderHolder defaultsHolder) {
      this.defaultsHolder = defaultsHolder;
   }

   protected ConfigurationBuilder getConfigurationBuilder(String name, String configuration) {
      Configuration cfg = super.getConfigurationBuilder(name, configuration).build();
      // Rebase the configuration on top of the defaults
      ConfigurationBuilder defaultCfg = defaultsHolder.getNamedConfigurationBuilders().get("org.infinispan." + cfg.clustering().cacheMode().name());
      ConfigurationBuilder rebased = new ConfigurationBuilder().read(defaultCfg.build());
      rebased.read(cfg);
      return rebased;
   }
}
