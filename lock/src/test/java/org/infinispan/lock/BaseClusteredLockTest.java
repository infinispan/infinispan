package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.test.MultipleCacheManagersTest;

public abstract class BaseClusteredLockTest extends MultipleCacheManagersTest {

   protected int clusterSize() {
      return 3;
   }

   protected GlobalConfigurationBuilder configure(int nodeId) {
      return GlobalConfigurationBuilder.defaultClusteredBuilder();
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      final int size = clusterSize();
      for (int i = 0; i < size; ++i) {
         addClusterEnabledCacheManager(configure(i), getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC));
      }
      waitForClusteredLockCaches();
   }

   protected final void waitForClusteredLockCaches() {
      waitForClusterToForm(null, ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);
   }

   protected final ClusteredLockManager clusteredLockManager(int index) {
      return EmbeddedClusteredLockManagerFactory.from(manager(index));
   }
}
