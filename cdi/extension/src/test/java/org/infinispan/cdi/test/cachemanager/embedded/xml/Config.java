package org.infinispan.cdi.test.cachemanager.embedded.xml;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.io.InputStream;

/**
 * Creates a number of caches, based on some external mechanism.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Associates the "very-large" cache (configured below) with the qualifier {@link VeryLarge}.</p>
    *
    * <p>The default configuration defined in "infinispan.xml" will be used.</p>
    */
   @VeryLarge
   @ConfigureCache("very-large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration veryLargeConfiguration;

   /**
    * Associates the "quick-very-large" cache (configured below) with the qualifier {@link Quick}.
    */
   @Quick
   @ConfigureCache("quick-very-large")
   @Produces
   @SuppressWarnings("unused")
   public Configuration quickVeryLargeConfiguration;

   /**
    * Overrides the default embedded cache manager.
    */
   @Produces
   @ApplicationScoped
   @SuppressWarnings("unused")
   public EmbeddedCacheManager defaultCacheManager() throws IOException {
      EmbeddedCacheManager externalCacheContainerManager = TestCacheManagerFactory.fromXml("infinispan.xml");

      externalCacheContainerManager.defineConfiguration("quick-very-large", new ConfigurationBuilder()
            .read(externalCacheContainerManager.getDefaultCacheConfiguration())
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
