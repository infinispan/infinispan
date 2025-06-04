package org.infinispan.cdi.embedded.test.cache.specific;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

/**
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class Config {

   /**
    * Associates the "large" cache with the qualifier {@link Large}.
    *
    * @param cacheManager the specific cache manager associated to this cache. This cache manager is used to get the
    *                     default cache configuration.
    */
   @Large
   @ConfigureCache("large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration largeConfiguration(@Large EmbeddedCacheManager cacheManager) {
      return new ConfigurationBuilder()
            .read(cacheManager.getDefaultCacheConfiguration())
            .memory().maxCount(2000)
            .build();
   }

   /**
    * Associates the "small" cache with the qualifier {@link Small}.
    *
    * @param cacheManager the specific cache manager associated to this cache. This cache manager is used to get the
    *                     default cache configuration.
    */
   @Small
   @ConfigureCache("small")
   @Produces
   @SuppressWarnings("unused")
   public Configuration smallConfiguration(@Small EmbeddedCacheManager cacheManager) {
      return new ConfigurationBuilder()
            .read(cacheManager.getDefaultCacheConfiguration())
            .memory().maxCount(20)
            .build();
   }

   /**
    * Associates the "small" and "large" caches with this specific cache manager.
    */
   @Large
   @Small
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager specificCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().defaultCacheName("default");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(4000);
      holder.getNamedConfigurationBuilders().put("default", builder);

      return new DefaultCacheManager(holder, true);
   }

   /**
    * Stops cache manager.
    *
    * @param cacheManager to be stopped
    */
   @SuppressWarnings("unused")
   public void killCacheManager(@Disposes @Small @Large EmbeddedCacheManager cacheManager) {
      TestingUtil.killCacheManagers(cacheManager);
   }

}
