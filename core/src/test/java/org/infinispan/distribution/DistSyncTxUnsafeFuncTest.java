package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncTxUnsafeFuncTest", enabled = false)
public class DistSyncTxUnsafeFuncTest extends DistSyncTxFuncTest {
   public DistSyncTxUnsafeFuncTest() {
      sync = true;
      tx = true;
      testRetVals = false;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }
}
