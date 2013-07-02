package org.infinispan.distribution.rehash;

import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.SingleLeaveTest")
public class SingleLeaveTest extends RehashLeaveTestBase {
   Address leaverAddress;

   void performRehashEvent(boolean offline) {
      // cause a node to LEAVE.  Typically this is c4.
      leaverAddress = addressOf(c4);
      CacheContainer cm4 = c4.getCacheManager();
      cacheManagers.remove(cm4);
      caches.remove(c4);
      TestingUtil.killCacheManagers(cm4);
   }
}
