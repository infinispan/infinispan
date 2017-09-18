package org.infinispan.distribution.rehash;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.SingleJoinTest")
public class SingleJoinTest extends RehashTestBase {
   EmbeddedCacheManager joinerManager;
   Cache<Object, String> joiner;

   void performRehashEvent(boolean offline) {
      joinerManager = addClusterEnabledCacheManager(new TransportFlags().withFD(true));
      joinerManager.defineConfiguration(cacheName, configuration.build());
      joiner = joinerManager.getCache(cacheName);
   }

   void waitForRehashCompletion() {
      // need to block until this join has completed!
      List<Cache> allCaches = new ArrayList(caches);
      allCaches.add(joiner);
      TestingUtil.blockUntilViewsReceived(60000, allCaches);
      waitForClusterToForm(cacheName);

      cacheManagers.add(joinerManager);
      caches.add(joiner);
   }

   @Test(groups = "unstable", description = "ISPN-8276")
   @Override
   public void testNonTransactional() throws Throwable {
      super.testNonTransactionalStress();
   }
}
