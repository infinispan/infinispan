package org.infinispan.hibernate.cache.commons;

import java.util.Properties;

import org.hibernate.engine.jndi.spi.JndiService;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.EmbeddedCacheManagerProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;

/**
 * JNDI-based {@link EmbeddedCacheManagerProvider}.
 * Falls back to default implementation if jndi name was unspecified, or not found.
 * 
 * @author Paul Ferraro
 * @since 9.2
 */
public class JndiCacheManagerProvider implements EmbeddedCacheManagerProvider {
   private static final InfinispanMessageLogger LOGGER = InfinispanMessageLogger.Provider.getLog(JndiCacheManagerProvider.class);

   /**
    * Specifies the JNDI name under which the {@link EmbeddedCacheManager} to use is bound.
    * There is no default value -- the user must specify the property.
    */
   public static final String CACHE_MANAGER_RESOURCE_PROP = "hibernate.cache.infinispan.cachemanager";

   private final ServiceRegistry registry;
   private final EmbeddedCacheManagerProvider defaultProvider;

   public JndiCacheManagerProvider(ServiceRegistry registry, EmbeddedCacheManagerProvider defaultProvider) {
      this.registry = registry;
      this.defaultProvider = defaultProvider;
   }

   @Override
   public EmbeddedCacheManager getEmbeddedCacheManager(Properties properties) {
      String jndiName = ConfigurationHelper.getString(CACHE_MANAGER_RESOURCE_PROP, properties, null);
      if (jndiName != null) {
         EmbeddedCacheManager manager = (EmbeddedCacheManager) this.registry.getService(JndiService.class).locate(jndiName);
         if (manager == null) {
            LOGGER.warnf("%s not found, falling back to default cache manager", jndiName);
         } else {
            LOGGER.debugf("Using cache manager bound to %s", jndiName);
            return new ManagedCacheManager(manager);
         }
      }
      return this.defaultProvider.getEmbeddedCacheManager(properties);
   }


   private static class ManagedCacheManager extends AbstractDelegatingEmbeddedCacheManager {

      ManagedCacheManager(EmbeddedCacheManager manager) {
         super(manager);
      }

      @Override
      public void stop() {
         // Do not stop
      }

      @Override
      public void close() {
         // Do not close
      }
   }
}
