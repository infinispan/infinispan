package org.infinispan.integrationtests.cdi.weld;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

/**
 * Cache configuration
 *
 * @author Sebastian Laskawiec
 */
@ApplicationScoped
public class Config {

   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager defaultEmbeddedCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().defaultCacheName("cdi");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(100);
      holder.getNamedConfigurationBuilders().put("cdi", builder);

      return new DefaultCacheManager(holder, true);
   }

   /**
    * Stops cache manager.
    *
    * @param cacheManager to be stopped
    */
   @SuppressWarnings("unused")
   public void killCacheManager(@Disposes EmbeddedCacheManager cacheManager) {
      TestingUtil.killCacheManagers(cacheManager);
   }

}
