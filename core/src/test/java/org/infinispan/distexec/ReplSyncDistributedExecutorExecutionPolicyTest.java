package org.infinispan.distexec;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Distributed Executor tests for Repl. sync mode verifying the execution policy.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.ReplSyncDistributedExecutorExecutionPolicyTest")
public class ReplSyncDistributedExecutorExecutionPolicyTest extends DistributedExecutorExecutionPolicyTest {

   /**
    * Returns the cache mode for the current cache configuration.
    * @return        the Cache mode.
    */
   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

}
