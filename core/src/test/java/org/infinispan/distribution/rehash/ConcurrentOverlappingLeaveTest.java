package org.infinispan.distribution.rehash;

import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentOverlappingLeaveTest", enabled = false, description = "Left out for now until new, push-based rehash is implemented")
public class ConcurrentOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;

   void performRehashEvent(boolean offline) {
      l1 = addressOf(c3);
      l2 = addressOf(c4);

      CacheContainer cm3 = c3.getCacheManager();
      CacheContainer cm4 = c4.getCacheManager();

      cacheManagers.removeAll(Arrays.asList(cm3, cm4));
      caches.removeAll(Arrays.asList(c3, c4));

      TestingUtil.killCacheManagers(cm3, cm4);
   }
}
