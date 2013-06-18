package org.infinispan.cdi;

import org.infinispan.cdi.util.defaultbean.DefaultBean;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Instance;
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

   private static final Log log = LogFactory.getLog(DefaultEmbeddedCacheManagerProducer.class, Log.class);

   /**
    * Produces the default embedded cache manager.
    *
    * @param providedDefaultEmbeddedCacheManager the provided default embedded cache manager.
    * @param defaultConfiguration the default configuration produced by the {@link DefaultEmbeddedCacheConfigurationProducer}.
    * @return the default embedded cache manager used by the application.
    */
   @Produces
   @ApplicationScoped
   @DefaultBean(EmbeddedCacheManager.class)
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(@OverrideDefault Instance<EmbeddedCacheManager> providedDefaultEmbeddedCacheManager, Configuration defaultConfiguration) {
      if (!providedDefaultEmbeddedCacheManager.isUnsatisfied()) {
         log.tracef("Default embedded cache manager overridden by '%s'", providedDefaultEmbeddedCacheManager);
         return providedDefaultEmbeddedCacheManager.get();
      }
      return new DefaultCacheManager(defaultConfiguration);
   }

   /**
    * Stops the default embedded cache manager when the corresponding instance is released.
    *
    * @param defaultEmbeddedCacheManager the default embedded cache manager.
    */
   private void stopCacheManager(@Disposes EmbeddedCacheManager defaultEmbeddedCacheManager) {
      defaultEmbeddedCacheManager.stop();
   }
}
