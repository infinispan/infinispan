package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleLocksTransactionTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksTransactionTest extends MultipleCacheManagersTest {

   Cache<String, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg
         .locking().lockAcquisitionTimeout(100)
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
   }

   public void testNoModsCommit() throws Exception {
      doTest(false, true);
   }

   public void testModsRollback() throws Exception {
      doTest(true, false);
   }

   public void testNoModsRollback() throws Exception {
      doTest(false, false);
   }

   public void testModsCommit() throws Exception {
      doTest(true, true);
   }

   private void doTest(boolean mods, boolean commit) throws Exception {
      tm(c1).begin();
      c1.getAdvancedCache().lock("k");
      assert c1.get("k") == null;
      if (mods) c1.put("k", "v");
      if (commit)
         tm(c1).commit();
      else
         tm(c1).rollback();

      assertEventuallyNotLocked(c1, "k");
      assertEventuallyNotLocked(c2, "k");
   }
}
