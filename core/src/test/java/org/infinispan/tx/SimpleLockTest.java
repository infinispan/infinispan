package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test (testName = "tx.SimpleLockTest", groups = "functional")
public class SimpleLockTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);

      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testA() throws Exception {
      Object a = getKeyForCache(1);
      tm(0).begin();
      cache(0).put("x", "b");
      advancedCache(0).lock(a);
      assertKeyLockedCorrectly(a);
      tm(0).commit();
   }
}
