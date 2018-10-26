package org.infinispan.cdi.embedded.test.cache.specific;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

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
            .memory().size(2000)
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
            .memory().size(20)
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
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().size(4000);
      return new DefaultCacheManager(builder.build());
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
