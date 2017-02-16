package org.infinispan.api;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.CacheClusterJoinTest")
public class CacheClusterJoinTest extends MultipleCacheManagersTest {

   private EmbeddedCacheManager cm1, cm2;
   private ConfigurationBuilder cfg;

   public CacheClusterJoinTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      cm1 = addClusterEnabledCacheManager();
      cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      cm1.defineConfiguration("cache", cfg.build());
   }

   public void testGetMembers() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      List memb1 = cm1.getMembers();
      assert 1 == memb1.size() : "Expected 1 member; was " + memb1;

      Object coord = memb1.get(0);

      cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("cache", cfg.build());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      TestingUtil.blockUntilViewsReceived(50000, true, cm1, cm2);
      memb1 = cm1.getMembers();
      List memb2 = cm2.getMembers();
      assert 2 == memb1.size();
      assert memb1.equals(memb2);

      TestingUtil.killCacheManagers(cm1);
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      memb2 = cm2.getMembers();
      assert 1 == memb2.size();
      assert !coord.equals(memb2.get(0));
   }

   public void testIsCoordinator() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator() : "Should be coordinator!";

      cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("cache", cfg.build());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator();
      assert !cm2.isCoordinator();
      TestingUtil.killCacheManagers(cm1);
      // wait till cache2 gets the view change notification
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      assert cm2.isCoordinator();
   }
}
