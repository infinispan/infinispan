package org.infinispan.cdi.test.cache.embedded.specific;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
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
            .eviction().maxEntries(2000)
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
            .eviction().maxEntries(20)
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
      builder.eviction().maxEntries(4000).strategy(EvictionStrategy.LIRS);
      return TestCacheManagerFactory.createCacheManager(builder);
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
