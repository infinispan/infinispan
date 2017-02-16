package org.infinispan.replication;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.AsyncAPITxAsyncReplTest")
public class AsyncAPITxAsyncReplTest extends AsyncAPITxSyncReplTest {

   private ReplListener rl;
   private ReplListener rlNoTx;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      rl = new ReplListener(cache(1), true);
      defineConfigurationOnAllManagers("noTx", new ConfigurationBuilder().read(manager(0).getDefaultCacheConfiguration()));
      rlNoTx = new ReplListener(cache(1, "noTx"), true);
   }

   @Override
   protected void asyncWait(boolean tx, Class<? extends WriteCommand>... cmds) {
      if (tx) {
         if (cmds == null || cmds.length == 0)
            rl.expectAnyWithTx();
         else
            rl.expectWithTx(cmds);
         rl.waitForRpc();
      } else {
         if (cmds == null || cmds.length == 0)
            rlNoTx.expectAny();
         else
            rlNoTx.expect(cmds);
         rlNoTx.waitForRpc();
      }
   }
}
