package org.infinispan.manager;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.util.TimeService;

/**
 * A CacheManager which performs operations across the whole cluster
 *
 * @author Tristan Tarrant
 * @since 9.1
 */

public class DefaultClusterCacheManager implements ClusterCacheManager {
   private final EmbeddedCacheManager cacheManager;
   private final GlobalConfigurationManager globalConfigurationManager;
   private final TimeService timeService;
   private final AuthorizationHelper authzHelper;

   DefaultClusterCacheManager(EmbeddedCacheManager cm, AuthorizationHelper authzHelper) {
      this.cacheManager = cm;
      this.authzHelper = authzHelper;
      globalConfigurationManager = cm.getGlobalComponentRegistry().getComponent(GlobalConfigurationManager.class);
      timeService = cm.getGlobalComponentRegistry().getComponent(TimeService.class);
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, Configuration configuration) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      if (!configuration.clustering().cacheMode().isClustered()) {
         // Prevent non-clustered cache creation
      }
      System.err.printf("Initiating clustered cache creation on '%s' on '%s'\n", cacheName, cacheManager.getAddress());
      globalConfigurationManager.createCache(cacheName, configuration);
      Cache<K, V> cache = cacheManager.getCache(cacheName);
      waitCacheViewComplete(cache, configuration);
      return cache;
   }

   private void waitCacheViewComplete(Cache<?, ?> cache, Configuration configuration) {
      long endTime = timeService.expectedEndTime(configuration.clustering().remoteTimeout(), TimeUnit.MILLISECONDS);
      do {
         if (cache.getAdvancedCache().getRpcManager().getMembers().containsAll(cacheManager.getTransport().getMembers()))
            return;
      } while (!timeService.isTimeExpired(endTime));
      throw new CacheException("Timeout");
   }

   @Override
   public void removeCache(String cacheName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      globalConfigurationManager.removeCache(cacheName);
   }

   @Override
   public ClusterExecutor executor() {
      return cacheManager.executor();
   }

   @Override
   public ClusterContainerStats getStats() {
      return cacheManager.getGlobalComponentRegistry().getComponent(ClusterContainerStats.class);
   }
}
