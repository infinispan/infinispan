package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistAsyncTxFuncTest", enabled = false)
public class DistAsyncTxFuncTest extends DistSyncTxFuncTest {

   public DistAsyncTxFuncTest() {
      sync = false;
      tx = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   @Override
   protected void asyncWait() {
      // we need to wait for an async event to happen on *each* cache?
      // TODO figure this out properly!
      try {
         Thread.sleep(2000);
      } catch (InterruptedException e) {
         e.printStackTrace();  // TODO: Customise this generated block
      }
   }
}