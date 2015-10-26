package org.infinispan.integrationtests.cdi.weld;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

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
      return new DefaultCacheManager(new ConfigurationBuilder()
            .eviction()
            .strategy(EvictionStrategy.LRU)
            .maxEntries(100)
            .build());
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
