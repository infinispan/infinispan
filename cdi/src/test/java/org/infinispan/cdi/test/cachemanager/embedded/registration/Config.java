package org.infinispan.cdi.test.cachemanager.embedded.registration;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Defines the "small" cache configuration.</p>
    *
    * <p>This cache will be registered with the default configuration of the default cache manager.</p>
    */
   @Small
   @ConfigureCache("small")
   @Produces
   @SuppressWarnings("unused")
   public Configuration smallConfiguration;

   /**
    * <p>Defines the "large" cache configuration.</p>
    *
    * <p>This cache will be registered with the produced configuration in the default cache manager.</p>
    */
   @Large
   @ConfigureCache("large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration largeConfiguration() {
      return new ConfigurationBuilder()
            .eviction().maxEntries(1024)
            .build();
   }

   /**
    * <p>Defines the "very-large" cache configuration.</p>
    *
    * <p>This cache will be registered with the produced configuration in the specific cache manager.</p>
    */
   @VeryLarge
   @ConfigureCache("very-large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration veryLargeConfiguration() {
      return new ConfigurationBuilder()
            .eviction().maxEntries(4096)
            .build();
   }

   /**
    * <p>Produces the specific cache manager.</p>
    *
    * <p>The "very-large" cache is associated to the specific cache manager with the cache qualifier.</p>
    */
   @VeryLarge
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager specificCacheManager() {
      return TestCacheManagerFactory.createCacheManager();
   }
}
