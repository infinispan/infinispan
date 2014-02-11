package org.infinispan.distexec;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tests org.infinispan.distexec.DistributedExecutorService in REPL_SYNC mode
 * 
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distexec.ReplSyncDistributedExecutorTest")
public class ReplSyncDistributedExecutorTest extends DistributedExecutorTest {

   public ReplSyncDistributedExecutorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected String cacheName() {
      return "DistributedExecutorTest-REPL_SYNC";
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Test(groups = "unstable")
   @Override
   public void testBasicTargetRemoteDistributedCallableWithHighFutureAndLowTaskTimeout() throws Exception {
      super.testBasicTargetRemoteDistributedCallableWithHighFutureAndLowTaskTimeout();
   }
}


