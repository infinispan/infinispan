package org.infinispan.all.embedded.util;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * <p>
 * The alternative default {@link org.infinispan.manager.EmbeddedCacheManager} producer for the test
 * environment.
 * </p>
 * 
 * @author Galder Zamarreño
 */
public class DefaultTestEmbeddedCacheManagerProducer {

   /**
    * Produces the default embedded cache manager.
    * 
    * @param providedDefaultEmbeddedCacheManager
    *           the provided default embedded cache manager.
    * @param defaultConfiguration
    *           the default configuration produced by the
    *           {@link DefaultTestEmbeddedCacheManagerProducer}.
    * @return the default embedded cache manager used by the application.
    */
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(
         Instance<EmbeddedCacheManager> providedDefaultEmbeddedCacheManager, Configuration defaultConfiguration) {
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().globalJmxStatistics()
            .allowDuplicateDomains(true).build();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(defaultConfiguration);

      return new DefaultCacheManager(globalConfiguration, builder.build());
   }

   /**
    * Stops the default embedded cache manager when the corresponding instance is released.
    * 
    * @param defaultEmbeddedCacheManager
    *           the default embedded cache manager.
    */
   private void stopCacheManager(@Disposes EmbeddedCacheManager defaultEmbeddedCacheManager) {
      defaultEmbeddedCacheManager.stop();
   }
}