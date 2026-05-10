package org.infinispan.lock.singlelock.optimistic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractNoCrashTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional",testName = "lock.singlelock.optimistic.BasicSingleLockOptimisticTest")
public class BasicSingleLockOptimisticTest extends AbstractNoCrashTest {

   public BasicSingleLockOptimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC, false);
   }

    protected void testTxAndLockOnDifferentNodes(Operation operation, boolean addFirst, boolean removed) throws Exception {
      final Object k = getKeyForCache(1);

      if (addFirst)
         cache(0).put(k, "v_initial");

      assertNotLocked(k);

      tm(0).begin();
      operation.perform(k, 0);
       EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).getTransaction();
      dtm.runPrepare();

      assertFalse(lockManager(0).isLocked(k));
      assertTrue(lockManager(1).isLocked(k));
      assertFalse(lockManager(2).isLocked(k));

      dtm.runCommit(false);

      assertNotLocked(k);

      assertValue(k, removed);
   }

   public void testMultipleLocksInSameTx() throws Exception {
      final Object k1 = getKeyForCache(1);
      final Object k2 = getKeyForCache(2);

      assertEquals(cacheTopology(0).getDistribution(k1).primary(), address(1));
      log.tracef("k1=%s, k2=%s", k1, k2);

      tm(0).begin();
      cache(0).put(k1, "v");
      cache(0).put(k2, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).getTransaction();
      dtm.runPrepare();

      assertFalse(lockManager(0).isLocked(k1));
      assertTrue(lockManager(1).isLocked(k1));
      assertFalse(lockManager(2).isLocked(k1));
      assertFalse(lockManager(0).isLocked(k2));
      assertFalse(lockManager(1).isLocked(k2));
      assertTrue(lockManager(2).isLocked(k2));

      dtm.runCommit(false);

      assertNotLocked(k1);
      assertNotLocked(k2);
      assertValue(k1, false);
      assertValue(k2, false);
   }

   public void testSecondTxCannotPrepare() throws Exception {
      final Object k = getKeyForCache(0);

      tm(0).begin();
      cache(0).put(k, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).getTransaction();
      dtm.runPrepare();
      tm(0).suspend();

      assertTrue(checkTxCount(0, 1, 0));
      assertTrue(checkTxCount(1, 0, 1));
      assertTrue(checkTxCount(2, 0, 1));

      tm(0).begin();
      cache(0).put(k, "other");
      try {
         tm(0).commit();
         fail();
      } catch (Throwable e) {
         //ignore
      }

      eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 1) && checkTxCount(2, 0, 1));


      log.info("Before second failure");
      tm(1).begin();
      cache(1).put(k, "other");
      try {
         tm(1).commit();
         fail();
      } catch (Throwable e) {
         //expected
      }

      eventually(() -> checkTxCount(0, 1, 0) && checkTxCount(1, 0, 1) && checkTxCount(2, 0, 1));


      tm(0).resume(dtm);
      dtm.runCommit(false);

      assertValue(k, false);

      eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
   }
}
