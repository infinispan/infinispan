package org.infinispan.api;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.ParallelCacheStartTest")
public class ParallelCacheStartTest extends MultipleCacheManagersTest {

   private EmbeddedCacheManager cm1, cm2;
   private ConfigurationBuilder cfg;

   public ParallelCacheStartTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      cm1 = addClusterEnabledCacheManager();
      cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      cm1.defineConfiguration("cache1", cfg.build());
      cm1.defineConfiguration("cache2", cfg.build());
   }

   public void testParallelStartup() throws Exception {
      // start both caches in parallel
      cm1.startCaches("cache1", "cache2");
      List memb1 = cm1.getMembers();
      assert 1 == memb1.size() : "Expected 1 member; was " + memb1;

      Object coord = memb1.get(0);

      cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("cache1", cfg.build());
      cm2.defineConfiguration("cache2", cfg.build());

      // again start both caches in parallel
      cm2.startCaches("cache1", "cache2");

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
}
