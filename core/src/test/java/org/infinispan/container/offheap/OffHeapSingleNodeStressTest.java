package org.infinispan.container.offheap;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 */
@Test(groups = "stress", testName = "container.offheap.OffHeapSingleNodeStressTest", timeOut = 15*60*1000)
public class OffHeapSingleNodeStressTest extends OffHeapMultiNodeStressTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dcc.memory().storage(StorageType.OFF_HEAP);
      // Only start up the 1 cache
      addClusterEnabledCacheManager(dcc);
   }
}
