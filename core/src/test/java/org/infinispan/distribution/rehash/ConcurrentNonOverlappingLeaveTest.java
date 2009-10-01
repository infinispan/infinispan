package org.infinispan.distribution.rehash;

import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentNonOverlappingLeaveTest")
public class ConcurrentNonOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;

   void performRehashEvent() {
      l1 = addressOf(c2);
      l2 = addressOf(c4);

      CacheManager cm2 = c2.getCacheManager();
      CacheManager cm4 = c4.getCacheManager();

      cacheManagers.removeAll(Arrays.asList(cm2, cm4));
      caches.removeAll(Arrays.asList(c2, c4));

      TestingUtil.killCacheManagers(cm2, cm4);
   }
}
