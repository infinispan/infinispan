package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistAsyncTxUnsafeFuncTest", enabled = false)
public class DistAsyncTxUnsafeFuncTest extends DistAsyncTxFuncTest {
   public DistAsyncTxUnsafeFuncTest() {
      sync = false;
      tx = true;
      testRetVals = false;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }
}
