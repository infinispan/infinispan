package org.infinispan.replication;

import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "replication.AsyncAPINonTxAsyncReplTest")
public class AsyncAPINonTxAsyncReplTest extends AsyncAPINonTxSyncReplTest {

   private ReplListener rl;
   private ReplListener rlNoTx;

   @Override
   protected boolean sync() {
      return false;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      rl = new ReplListener(cache(1), true);
      rlNoTx = new ReplListener(cache(1, "noTx"), true);
   }

   @Override
   protected void resetListeners() {
      rl.resetEager();
   }

}
