package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.AsyncAPINonTxAsyncDistTest")
public class AsyncAPINonTxAsyncDistTest extends AsyncAPINonTxSyncDistTest {

   @Override
   protected boolean sync() {
      return false;
   }
}
