package org.infinispan.tx.dld;

import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "tx.EagerTxDeadlockDetectionTest")
public class DldEagerLockingReplicationTest extends BaseDldEagerLockingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getConfiguration();
      createClusteredCaches(2, configuration);
      TestingUtil.blockUntilViewsReceived(1000, cache(0), cache(1));
   }

   protected Configuration getConfiguration() throws Exception {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      configuration.setUseEagerLocking(true);
      configuration.setEnableDeadlockDetection(true);
      configuration.setUseLockStriping(false);
      return configuration;
   }

   public void testDeadlock() throws Exception {
      testSymmetricDld("k1", "k2");
   }

   /**
    * On eager locking, remote locks are being acquired at first, and then local locks. This is for specifying the
    * behavior whe remote acquisition succeeds and local fails.
    */
   public void testDeadlockFailedToAcquireLocalLocks() throws Exception {
      //first acquire a local lock on k1
      TransactionManager tm = TestingUtil.getTransactionManager(cache(0));
      tm.begin();
      cache(0).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put("k1","v1");
      assert lm0.isLocked("k1");
      assert !lm1.isLocked("k1");
      try {
         ex0.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
         ex0.setKeyValue("k1", "v1_1");
         ex0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
         assert ex0.lastResponse() instanceof TimeoutException;
         eventually(new Condition() {
            public boolean isSatisfied() throws Exception {
               return !lm1.isLocked("k1");
            }
         });
      } finally {
         tm.rollback();
      }
   }
}
