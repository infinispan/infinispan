package org.infinispan.factories;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Factory for RecoveryManager.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {RecoveryManager.class})
public class RecoveryManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final long DEFAULT_EXPIRY = TimeUnit.HOURS.toMillis(6);

   @Override
   @SuppressWarnings("unchecked")
   public <RecoveryManager> RecoveryManager construct(Class<RecoveryManager> componentType) {
      checkAsyncCache(configuration);
      boolean recoveryEnabled = configuration.isTransactionRecoveryEnabled();
      String cacheName = configuration.getName() == null ? CacheContainer.DEFAULT_CACHE_NAME : configuration.getName();
      if (recoveryEnabled) {
         String recoveryCacheName = configuration.getTransactionRecoveryCacheName();
         if (log.isTraceEnabled()) log.trace("Using recovery cache name %s", recoveryCacheName);
         EmbeddedCacheManager cm = componentRegistry.getGlobalComponentRegistry().getComponent(EmbeddedCacheManager.class);
         boolean useDefaultCache = recoveryCacheName.equals(Configuration.RecoveryConfig.DEFAULT_RECOVERY_INFO_CACHE);

         //if use a defined cache
         if (!useDefaultCache) {
            // check to see that the cache is defined
            if (!cm.getCacheNames().contains(recoveryCacheName)) {
               throw new ConfigurationException("Recovery cache (" + recoveryCacheName + ") does not exist!!");
            }
          } else {
            //this might have already been defined by other caches
            if (!cm.getCacheNames().contains(recoveryCacheName)) {
               Configuration config = getDefaultRecoveryCacheConfig();
               cm.defineConfiguration(recoveryCacheName, config);
            }
         }
         return (RecoveryManager) withRecoveryCache(cacheName, recoveryCacheName, cm);
      } else {
         return null;
      }
   }

   private void checkAsyncCache(Configuration configuration) {
      if (configuration.isTransactionRecoveryEnabled() && configuration.isOnePhaseCommit()) {
         throw new ConfigurationException("Recovery for async caches is not supported!");
      }
   }

   private Configuration getDefaultRecoveryCacheConfig() {
      Configuration config = new Configuration();
      config.configureClustering().mode(Configuration.CacheMode.LOCAL);
      config.configureExpiration().lifespan(DEFAULT_EXPIRY);
      config.configureTransaction().configureRecovery().enabled(false);
      return config;
   }

   private RecoveryManager withRecoveryCache(String cacheName, String recoveryCacheName, EmbeddedCacheManager cm) {
      Cache recoveryCache = cm.getCache(recoveryCacheName);
      return new RecoveryManagerImpl(recoveryCache,  cacheName);
   }
}
