package org.infinispan.cdi;

import org.infinispan.cdi.util.defaultbean.DefaultBean;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * <p>The default {@link EmbeddedCacheManager} producer.</p>
 *
 * <p>The cache manager produced by default is an instance of {@link DefaultCacheManager} initialized with the default
 * configuration produced by the {@link DefaultEmbeddedCacheConfigurationProducer}. The default cache manager can be
 * overridden by creating a producer which produces the new default cache manager. The cache manager produced must have
 * the scope {@link ApplicationScoped} and the {@linkplain javax.enterprise.inject.Default Default} qualifier.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultEmbeddedCacheManagerProducer {

   /**
    * Produces the default embedded cache manager.
    *
    * @param defaultConfiguration the default configuration produced by the {@link DefaultEmbeddedCacheConfigurationProducer}.
    * @return the default embedded cache manager used by the application.
    */
   @Produces
   @ApplicationScoped
   @DefaultBean(EmbeddedCacheManager.class)
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(Configuration defaultConfiguration) {
      return new DefaultCacheManager(defaultConfiguration);
   }

   /**
    * Stops the default embedded cache manager when the corresponding instance is released.
    *
    * @param defaultEmbeddedCacheManager the default embedded cache manager.
    */
   @SuppressWarnings("unused")
   private void stopCacheManager(@Disposes EmbeddedCacheManager defaultEmbeddedCacheManager) {
      defaultEmbeddedCacheManager.stop();
   }
}
