package org.infinispan.distribution.rehash;

import java.util.Arrays;

import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentOverlappingLeaveTest")
public class ConcurrentOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;

   {
      // since we have two leavers, for some keys we're going to lose 2 owners
      // we set numOwners to 3 so that all keys will have at least 1 owner remaining
      numOwners = 3;
   }

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
