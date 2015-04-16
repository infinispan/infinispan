package org.infinispan.lock.singlelock.pessimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractNoCrashTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.pessimistic.BasicSingleLockPessimisticTest")
public class BasicSingleLockPessimisticTest extends AbstractNoCrashTest {

   public BasicSingleLockPessimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.PESSIMISTIC, false);
   }

   protected void testTxAndLockOnDifferentNodes(Operation operation, boolean addFirst, boolean removed) throws Exception {
      final Object k = getKeyForCache(1);

      if (addFirst)
         cache(0).put(k, "v_initial");

      assertNotLocked(k);

      tm(0).begin();
      operation.perform(k, 0);

      assert !lockManager(0).isLocked(k);
      assert lockManager(1).isLocked(k);
      assert !lockManager(2).isLocked(k);

      tm(0).commit();

      assertNotLocked(k);

      assertValue(k, removed);
   }

   public void testMultipleLocksInSameTx() throws Exception {
      final Object k1 = getKeyForCache(1);
      final Object k2 = getKeyForCache(2);

      assertEquals(advancedCache(0).getDistributionManager().locate(k1).get(0), address(1));
      log.tracef("k1=%s, k2=%s", k1, k2);

      tm(0).begin();
      cache(0).put(k1, "v");
      cache(0).put(k2, "v");

      assert !lockManager(0).isLocked(k1);
      assert lockManager(1).isLocked(k1);
      assert !lockManager(2).isLocked(k1);
      assert !lockManager(0).isLocked(k2);
      assert !lockManager(1).isLocked(k2);
      assert lockManager(2).isLocked(k2);

      tm(0).commit();

      assertNotLocked(k1);
      assertNotLocked(k2);
      assertValue(k1, false);
      assertValue(k2, false);
   }

   public void testSecondTxCannotPrepare() throws Exception {
      final Object k = getKeyForCache(0);
      final Object k1= getKeyForCache(1);

      tm(0).begin();
      cache(0).put(k, "v");
      DummyTransaction dtm = (DummyTransaction) tm(0).getTransaction();
      tm(0).suspend();

      assert checkTxCount(0, 1, 0);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 0);

      tm(0).begin();
      cache(0).put(k1, "some");
      try {
         cache(0).put(k, "other");
      } catch (Throwable e) {
         //ignore
      } finally {
         tm(0).rollback();
      }

      assertNotLocked(k1);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(2, 0, 0);
         }
      });


      log.info("Before second failure");
      tm(1).begin();
      cache(1).put(k1, "some");
      try {
         cache(1).put(k, "other");
         assert false;
      } catch (Throwable e) {
         //expected
      } finally {
         tm(1).rollback();
      }
      assertNotLocked(k1);

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 1, 0) && checkTxCount(1, 0, 0) && checkTxCount(1, 0, 0);
         }
      });


      log.trace("about to commit transaction.");
      tm(0).resume(dtm);
      tm(0).commit();

      assertValue(k, false);

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2);
         }
      });
   }
}
