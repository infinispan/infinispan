package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "tx.EagerTxDeadlockDetectionTest")
public class EagerTxDeadlockDetectionTest extends MultipleCacheManagersTest {
   private PerCacheExecutorThread ex1;
   private PerCacheExecutorThread ex2;
   private DeadlockDetectingLockManager lm0;
   private DeadlockDetectingLockManager lm1;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getConfiguration();
      createClusteredCaches(2, configuration);
      ex1 = new PerCacheExecutorThread(cache(0), 1);
      ex2 = new PerCacheExecutorThread(cache(1), 2);
      lm0 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0));
      lm0.setExposeJmxStats(true);
      lm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1));
      lm1.setExposeJmxStats(true);
   }

   protected Configuration getConfiguration() throws Exception {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      configuration.setUseEagerLocking(true);
      configuration.setEnableDeadlockDetection(true);
      configuration.setUseLockStriping(false);
      return configuration;
   }

   public void testDeadlock() throws Exception {
      long start = System.currentTimeMillis();
      ex1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
      ex2.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);

      ex1.setKeyValue("k1", "v1_1");
      ex1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      ex2.setKeyValue("k2", "v2_2");
      ex2.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      assert lm0.isLocked("k1");
      assert lm0.isLocked("k2");
      assert lm1.isLocked("k1");
      assert lm1.isLocked("k2");

      log.trace("After first set of puts");

      ex1.clearResponse();
      ex2.clearResponse();

      log.info("Here is where DLD happens");

      ex2.setKeyValue("k1", "v1_2");
      ex2.executeNoResponse(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      ex1.setKeyValue("k2", "v2_1");
      ex1.executeNoResponse(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      ex1.waitForResponse();
      ex2.waitForResponse();


      boolean b1 = ex1.lastResponse() instanceof Exception;
      boolean b2 = ex2.lastResponse() instanceof Exception;
      log.info("b1:", b1);
      log.info("b2:", b2);
      assert xor(b1, b2) : "Both are " + (b1 || b2);

      assert xor(ex1.getOngoingTransaction().getStatus() == Status.STATUS_MARKED_ROLLBACK,
                 ex2.getOngoingTransaction().getStatus() == Status.STATUS_MARKED_ROLLBACK);


      ex1.execute(PerCacheExecutorThread.Operations.COMMIT_TX);
      ex2.execute(PerCacheExecutorThread.Operations.COMMIT_TX);


      assert cache(0).get("k1") != null;
      assert cache(0).get("k2") != null;
      assert cache(1).get("k1") != null;
      assert cache(1).get("k2") != null;

      long totalDeadlocks = lm0.getTotalNumberOfDetectedDeadlocks() + lm1.getTotalNumberOfDetectedDeadlocks();
      assert totalDeadlocks == 1 : "Expected 1 but received " + totalDeadlocks;                              

      System.out.println("Test took " + (System.currentTimeMillis() - start) + " millis.");
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
         ex1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
         ex1.setKeyValue("k1", "v1_1");
         ex1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
         assert ex1.lastResponse() instanceof TimeoutException;
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
