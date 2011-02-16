package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.RehashLeaveTestBase")
public abstract class RehashLeaveTestBase extends RehashTestBase {
   void waitForRehashCompletion() {
      RehashWaiter.waitForRehashToComplete(caches.toArray(new Cache[caches.size()]));
   }
}
