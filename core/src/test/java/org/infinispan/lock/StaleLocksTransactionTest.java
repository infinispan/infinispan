package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(testName = "lock.StaleLocksTransactionTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksTransactionTest extends MultipleCacheManagersTest {

   Cache<String, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      cfg.setLockAcquisitionTimeout(100);
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

      assertNotLocked(c1, "k");
      assertNotLocked(c2, "k");
   }
}
