package org.infinispan.distribution;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.AsyncAPITxAsyncDistTest")
public class AsyncAPITxAsyncDistTest extends AsyncAPITxSyncDistTest {

   private ReplListener rl;
   private ReplListener rlNoTx;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      rl = new ReplListener(cache(1), true);
      rlNoTx = new ReplListener(cache(1, "noTx"), true);
   }

   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_ASYNC, true);
   }

   @Override
   protected void asyncWait(boolean tx, Class<? extends WriteCommand>... cmds) {
      if (tx) {
         if (cmds == null || cmds.length == 0)
            rl.expectAnyWithTx();
         else
            rl.expectWithTx(cmds);
         rl.waitForRpc(240, TimeUnit.SECONDS);
      } else {
         if (cmds == null || cmds.length == 0)
            rlNoTx.expectAny();
         else
            rlNoTx.expect(cmds);
         rlNoTx.waitForRpc(240, TimeUnit.SECONDS);
      }
   }
}