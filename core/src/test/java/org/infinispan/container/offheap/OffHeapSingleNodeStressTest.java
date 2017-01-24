package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 */
@Test(groups = "stress", testName = "container.offheap.OffHeapSingleNodeStressTest")
public class OffHeapSingleNodeStressTest extends OffHeapMultiNodeStressTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      dcc.memory().storageType(StorageType.OFF_HEAP);
      // Only start up the 1 cache
      addClusterEnabledCacheManager(dcc);
   }
}
