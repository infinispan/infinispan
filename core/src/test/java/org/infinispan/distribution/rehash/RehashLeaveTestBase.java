package org.infinispan.distribution.rehash;

import org.infinispan.test.TestingUtil;

public abstract class RehashLeaveTestBase extends RehashTestBase {
   void waitForRehashCompletion() {
      TestingUtil.blockUntilViewsReceived(60000, false, caches);
      TestingUtil.waitForStableTopology(caches);
   }
}
