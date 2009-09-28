package org.infinispan.distribution.rehash;

import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentOverlappingLeaveTest", enabled = false)
public class ConcurrentOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;

   void performRehashEvent() {
      l1 = addressOf(c3);
      l2 = addressOf(c4);

      CacheManager cm3 = c3.getCacheManager();
      CacheManager cm4 = c4.getCacheManager();

      cacheManagers.removeAll(Arrays.asList(cm3, cm4));
      caches.removeAll(Arrays.asList(c3, c4));

      TestingUtil.killCacheManagers(cm3, cm4);
   }
}
