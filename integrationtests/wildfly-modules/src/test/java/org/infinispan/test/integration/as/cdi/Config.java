package org.infinispan.test.integration.as.cdi;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.DefaultCacheManager;

/**
 * This is the configuration class.
 *
 * @author Kevin Pollet &lt;pollet.kevin@gmail.com&gt; (C) 2011
 * @author Galder Zamarreño
 */
public class Config {

   /**
    * <p>This producer defines the greeting cache configuration.</p>
    *
    * <p>This cache will have:
    * <ul>
    *    <li>a maximum of 4 entries</li>
    *    <li>use the strategy LRU for eviction</li>
    * </ul>
    * </p>
    *
    * @return the greeting cache configuration.
    */
   @GreetingCache
   @ConfigureCache("greeting-cache")
   @Produces
   public Configuration greetingCache() {
      return new ConfigurationBuilder()
            .memory().storageType(StorageType.OBJECT).size(128)
            .build();
   }

   /**
    * <p>This producer overrides the default cache configuration used by the default cache manager.</p>
    *
    * <p>The default cache configuration defines that a cache entry will have a lifespan of 60000 ms.</p>
    */
   @Produces
   public Configuration defaultCacheConfiguration() {
      return new ConfigurationBuilder()
            .expiration().lifespan(60000l)
            .build();
   }

   @Produces
   @ApplicationScoped
   public org.infinispan.manager.EmbeddedCacheManager defaultEmbeddedCacheManager() {
      return new DefaultCacheManager();
   }

   public void killCacheManager(@Disposes org.infinispan.manager.EmbeddedCacheManager cacheManager) {
      cacheManager.stop();
   }

}
