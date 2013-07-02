package org.infinispan.tx.dld;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "tx.dld.DldPessimisticLockingReplicationTest")
public class DldPessimisticLockingReplicationTest extends BaseDldPessimisticLockingTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder configuration = getConfigurationBuilder();
      createClusteredCaches(2, configuration);
      TestingUtil.blockUntilViewsReceived(1000, cache(0), cache(1));
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.transaction().lockingMode(LockingMode.PESSIMISTIC)
            .locking().useLockStriping(false)
            .deadlockDetection().enable();
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
      tm(0).begin();
      cache(0).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put("k1","v1");
      assert lm0.isLocked("k1");
      assert !lm1.isLocked("k1");
      try {
         ex0.execute(PerCacheExecutorThread.Operations.BEGIN_TX);
         ex0.setKeyValue("k1", "v1_1");
         ex0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
         assert ex0.lastResponse() instanceof TimeoutException : "received " + ex0.lastResponse();
         eventually(new Condition() {
            public boolean isSatisfied() throws Exception {
               return !lm1.isLocked("k1");
            }
         });
      } finally {
         tm(0).rollback();
      }
   }
}
