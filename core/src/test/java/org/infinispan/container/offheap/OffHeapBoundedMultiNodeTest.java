package org.infinispan.container.offheap;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "container.offheap.OffHeapBoundedMultiNodeTest")
public class OffHeapBoundedMultiNodeTest extends OffHeapMultiNodeTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      dcc.memory().storageType(StorageType.OFF_HEAP).size(51);
      dcc.clustering().stateTransfer().timeout(10, TimeUnit.SECONDS);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }
}
