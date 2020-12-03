package org.infinispan.lock;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.configuration.ClusteredLockManagerConfigurationBuilder;
import org.infinispan.lock.configuration.Reliability;
import org.infinispan.lock.impl.ClusteredLockModuleLifecycle;
import org.infinispan.test.MultipleCacheManagersTest;

public abstract class BaseClusteredLockTest extends MultipleCacheManagersTest {

   protected Reliability reliability = Reliability.CONSISTENT;
   protected int numOwner = -1;

   protected int clusterSize() {
      return 3;
   }

   protected BaseClusteredLockTest numOwner(int numOwner) {
      this.numOwner = numOwner;
      return this;
   }

   protected GlobalConfigurationBuilder configure(int nodeId) {
      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.metrics().gauges(false);

      globalConfigurationBuilder
            .addModule(ClusteredLockManagerConfigurationBuilder.class)
            .numOwner(numOwner)
            .reliability(reliability);

      return globalConfigurationBuilder;
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      final int size = clusterSize();
      for (int i = 0; i < size; ++i) {
         addClusterEnabledCacheManager(configure(i), null);
      }
      waitForClusteredLockCaches();
   }

   protected final void waitForClusteredLockCaches() {
      waitForClusterToForm(ClusteredLockModuleLifecycle.CLUSTERED_LOCK_CACHE_NAME);
   }

   protected final ClusteredLockManager clusteredLockManager(int index) {
      return EmbeddedClusteredLockManagerFactory.from(manager(index));
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "lockOwners");
   }

   @Override
   protected Object[] parameterValues() {
      // Omit the numOwner parameter if it's 0
      Integer numOwnerParameter = numOwner != 0 ? numOwner : null;
      return concat(super.parameterValues(), numOwnerParameter);
   }
}
