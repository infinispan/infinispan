package org.infinispan.cdi.embedded.test.cachemanager.external;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * Creates a number of caches, based on some external mechanism.
 *
 * @author Pete Muir
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class Config {
   /**
    * Associates the externally defined "large" cache with the qualifier {@link Large}.
    */
   @Large
   @ConfigureCache("large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration largeConfiguration;

   /**
    * Associates the externally defined "quick" cache with the qualifier {@link Quick}.
    */
   @Quick
   @ConfigureCache("quick")
   @Produces
   @SuppressWarnings("unused")
   public Configuration quickConfiguration;

   /**
    * Overrides the default embedded cache manager to define the quick and large cache configurations externally.
    */
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager defaultCacheManager() {
      EmbeddedCacheManager externalCacheContainerManager = TestCacheManagerFactory.createCacheManager(false);

      // define large configuration
      externalCacheContainerManager.defineConfiguration("large", new ConfigurationBuilder()
            .memory().size(100)
            .build());

      // define quick configuration
      externalCacheContainerManager.defineConfiguration("quick", new ConfigurationBuilder()
            .expiration().wakeUpInterval(1l)
            .build());

      return externalCacheContainerManager;
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
