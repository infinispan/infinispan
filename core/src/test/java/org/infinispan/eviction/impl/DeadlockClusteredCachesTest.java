package org.infinispan.eviction.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "eviction.DeadlockClusteredCachesTest", timeOut = 1*60*1000)
public class DeadlockClusteredCachesTest extends SingleCacheManagerTest {

   protected ControlledTimeService timeService = new ControlledTimeService();

   protected int maxEntries;

   @Factory
   public Object[] factory() {
      int max = 10;
      Object[] data = new Object[max];
      for (int i = 0; i < max; i++) {
         data[i] = new DeadlockClusteredCachesTest().maxEntries(100 * (i + 1));
      }
      return data;
   }

   protected DeadlockClusteredCachesTest maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   @Override
   protected String parameters() {
      return "[" + maxEntries + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cfgBuilder.expiration().disableReaper();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(cfgBuilder);
      cache = cm.getCache();
      return cm;
   }

   public void testDeadlockReaper() throws InterruptedException {
      for (int i = 0; i < maxEntries; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1), -1, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(1000);
      Thread t1 = new Thread(() -> {
         cache.getAdvancedCache().getExpirationManager().processExpiration();
      });
      Thread t2 = new Thread(() -> {
         for (int i = 0; i < maxEntries; i++) {
            cache.get("key-" + (i + 1));
         }
      });
      t1.start();
      t2.start();
      t1.join();
      t2.join();
   }
}
