package org.infinispan.cdi.embedded.test.cachemanager.programmatic;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;

/**
 * Creates a cache, based on some external mechanism.
 *
 * @author Pete Muir
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class Config {

   /**
    * <p>Associates the "large" cache with qualifier "large"</p>
    *
    * Note that {@link Named} works as a string-based qualifier, so it is necessary to support also {@link ConfigureCache}.
    *
    * @see Named
    * @see ConfigureCache
    */
   @Named("large")
   @ConfigureCache("large")
   @Produces
   public Configuration largeConfiguration = new ConfigurationBuilder().memory().maxCount(10).build();

   /**
    * The same as the above. The intention here is to check whether we can use 2 Configurations with <code>@Named</code>
    * annotations.
    */
   @Named("super-large")
   @ConfigureCache("super-large")
   @Produces
   public Configuration superLargeConfiguration = new ConfigurationBuilder().memory().maxCount(20).build();

   /**
    * <p>Associates the "small" cache with the qualifier {@link Small}.</p>
    *
    * <p>The default configuration will be used.</p>
    */
   @Small
   @ConfigureCache("small")
   @Produces
   @SuppressWarnings("unused")
   public Configuration smallConfiguration;

   /**
    * Overrides the default embedded cache manager.
    */
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager defaultCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(7);
      return TestCacheManagerFactory.createCacheManager(builder);
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
