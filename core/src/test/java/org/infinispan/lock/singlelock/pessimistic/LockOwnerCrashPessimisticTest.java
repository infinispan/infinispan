package org.infinispan.lock.singlelock.pessimistic;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;


/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.pessimistic.LockOwnerCrashPessimisticTest")
@CleanupAfterMethod
public class LockOwnerCrashPessimisticTest extends AbstractLockOwnerCrashTest {

   public LockOwnerCrashPessimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.PESSIMISTIC, false);
   }

   public void testLockOwnerCrashesBeforePrepare() throws Exception {
      final Object k = getKeyForCache(2);
      inNewThread(() -> {
         try {
            tm(1).begin();
            cache(1).put(k, "v");
            transaction = (EmbeddedTransaction) tm(1).getTransaction();
         } catch (Throwable e) {
            log.errorf(e, "Error starting transaction for key %s", k);
         }
      });

      Eventually.eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));

      Eventually.eventually(() -> !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k));

      killMember(2);
      assert caches().size() == 2;

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("v", cache(0).get(k));
      assertEquals("v", cache(1).get(k));

      assertNotLocked(k);
      Eventually.eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
   }

   public void testLockOwnerCrashesBeforePrepareAndLockIsStillHeld() throws Exception {
      final Object k = getKeyForCache(2);
      inNewThread(() -> {
         try {
            tm(1).begin();
            cache(1).put(k, "v");
            transaction = (EmbeddedTransaction) tm(1).getTransaction();
         } catch (Throwable e) {
            log.errorf(e, "Error starting transaction for key %s", k);
         }
      });

      Eventually.eventually(() -> !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k));

      killMember(2);
      assert caches().size() == 2;

      tm(0).begin();
      try {
         cache(0).put(k, "v1");
         assert false;
      } catch (Exception e) {
         tm(0).rollback();
      }

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("v", cache(0).get(k));
      assertEquals("v", cache(1).get(k));

      assertNotLocked(k);
      Eventually.eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit1() throws Exception {
      testCrashBeforeCommit(true);
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit2() throws Exception {
      testCrashBeforeCommit(false);
   }

   private void testCrashBeforeCommit(final boolean crashBeforePrepare) throws NotSupportedException, SystemException, InvalidTransactionException, HeuristicMixedException, RollbackException, HeuristicRollbackException {
      final Object k = getKeyForCache(2);
      inNewThread(() -> {
         try {
            tm(1).begin();
            cache(1).put(k, "v");
            transaction = (EmbeddedTransaction) tm(1).getTransaction();
            if (!crashBeforePrepare) {
               transaction.runPrepare();
            }
         } catch (Throwable e) {
            log.errorf(e, "Error preparing transaction for key %s", k);
         }
      });


      Eventually.eventually(() -> checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));

      Eventually.eventually(() -> !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k));


      killMember(2);
      assertEquals(2, caches().size());


      tm(1).begin();
      try {
         cache(1).put(k, "v2");
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         tm(1).rollback();
      }

      tm(0).begin();
      try {
         cache(0).put(k, "v3");
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         tm(0).rollback();
      }


      tm(1).resume(transaction);
      if (!crashBeforePrepare) {
         transaction.runCommit(false);
      } else {
         tm(1).commit();
      }
      assertEquals("v", cache(0).get(k));
      assertEquals("v", cache(1).get(k));
      assertNotLocked(k);
   }
}
