package org.horizon.api;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.Configuration.CacheMode;
import org.horizon.manager.CacheManager;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "functional", testName = "api.CacheClusterJoinTest", enabled = false)
public class CacheClusterJoinTest extends MultipleCacheManagersTest {
   Cache cache1, cache2;
   CacheManager cm1, cm2;
   Configuration cfg;

   public CacheClusterJoinTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      cm1 = addClusterEnabledCacheManager();
      cfg = new Configuration();
      cfg.setCacheMode(CacheMode.REPL_SYNC);
      cfg.setFetchInMemoryState(false);
      cm1.defineCache("cache", cfg);
   }

   public void testGetMembers() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      List memb1 = cm1.getMembers();
      assert 1 == memb1.size() : "Expected 1 member; was " + memb1;

      Object coord = memb1.get(0);

      cm2 = addClusterEnabledCacheManager();
      cm2.defineCache("cache", cfg.clone());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      TestingUtil.blockUntilViewsReceived(50000, true, cm1, cm2);
      memb1 = cm1.getMembers();
      List memb2 = cm2.getMembers();
      assert 2 == memb1.size();
      assert memb1.equals(memb2);

      cm1.stop();
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      memb2 = cm2.getMembers();
      assert 1 == memb2.size();
      assert !coord.equals(memb2.get(0));
   }

   public void testIsCoordinator() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator() : "Should be coordinator!";

      cm2 = addClusterEnabledCacheManager();
      cm2.defineCache("cache", cfg.clone());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator();
      assert !cm2.isCoordinator();
      cm1.stop();
      // wait till cache2 gets the view change notification
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      assert cm2.isCoordinator();
   }
}
