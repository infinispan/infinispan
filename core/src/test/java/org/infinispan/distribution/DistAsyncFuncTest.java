package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistAsyncFuncTest", enabled = false)
public class DistAsyncFuncTest extends DistSyncFuncTest {

   public DistAsyncFuncTest() {
      sync = false;
      tx = false;
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