package org.infinispan.cdi.embedded.test;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.commons.test.TestResourceTracker;

/**
 * <p>The alternative default {@link EmbeddedCacheManager} producer for the test environment.</p>
 *
 * @author Galder Zamarreño
 */
public class DefaultTestEmbeddedCacheManagerProducer {

   /**
    * Produces the default embedded cache manager.
    *
    * @param defaultConfiguration the default configuration produced by the {@link DefaultTestEmbeddedCacheManagerProducer}.
    * @return the default embedded cache manager used by the application.
    */
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(Configuration defaultConfiguration) {
      // Sometimes we're called from a remote thread that doesn't have the test name set
      // We don't have the test name here, either, but we can use a dummy one
      TestResourceTracker.setThreadTestNameIfMissing("DefaultTestEmbeddedCacheManagerProducer");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      builder.read(defaultConfiguration);
      EmbeddedCacheManager manager = createClusteredCacheManager(globalConfigurationBuilder, builder);
      // Defie a wildcard configuration for the CDI integration tests
      manager.defineConfiguration("org.infinispan.integrationtests.cdijcache.interceptor.service.Cache*Service*",
                                  new ConfigurationBuilder().template(true).build());
      return manager;
   }

   /**
    * Stops the default embedded cache manager when the corresponding instance is released.
    *
    * @param defaultEmbeddedCacheManager the default embedded cache manager.
    */
   private void stopCacheManager(@Disposes EmbeddedCacheManager defaultEmbeddedCacheManager) {
      TestingUtil.killCacheManagers(defaultEmbeddedCacheManager);
   }

}
