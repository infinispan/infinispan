package org.infinispan.tx;


import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "tx.Use1PcForInducedTxLockingTest")
public class Use1PcForInducedTxLockingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().use1PcForAutoCommitTransactions(true);
      dcc.clustering().hash().numOwners(1);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testCorrectLocking() {
      Object k0 = getKeyForCache(0);
      cache(1).put(k0, "v0");
      assertNotLocked(k0);
      assertNoTransactions();
      cache(0).put(k0, "v0");
      assertNotLocked(k0);
      assertNoTransactions();
   }
}
