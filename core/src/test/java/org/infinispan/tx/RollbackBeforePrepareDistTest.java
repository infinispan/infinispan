package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

@Test(testName = "tx.RollbackBeforePrepareDistTest", groups = "functional")
public class RollbackBeforePrepareDistTest extends RollbackBeforePrepareTest {

   public RollbackBeforePrepareDistTest() {
      cacheMode = CacheMode.DIST_SYNC;
      numOwners = 3;
   }
}
