package org.infinispan.lock.singlelock.pessimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

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
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });

      killMember(2);
      assertEquals("Wrong number of caches", 2, caches().size());

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("Wrong value for key 'k' in " + cache(0), "v", cache(0).get(k));
      assertEquals("Wrong value for key 'k' in " + cache(1), "v", cache(1).get(k));

      assertNotLocked(k);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void testLockOwnerCrashesBeforePrepareAndLockIsStillHeld() throws Exception {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });

      killMember(2);
      assertEquals("Wrong number of caches", 2, caches().size());

      tm(0).begin();
      try {
         cache(0).put(k, "v1");
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         tm(0).rollback();
      }

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("Wrong value for key 'k' in " + cache(0), "v", cache(0).get(k));
      assertEquals("Wrong value for key 'k' in " + cache(1), "v", cache(1).get(k));

      assertNotLocked(k);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit1() throws Exception {
      testCrashBeforeCommit(true);
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit2() throws Exception {
      testCrashBeforeCommit(false);
   }

   private void testCrashBeforeCommit(final boolean crashBeforePrepare) throws NotSupportedException, SystemException, InvalidTransactionException, HeuristicMixedException, RollbackException, HeuristicRollbackException {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               if (!crashBeforePrepare) {
                  transaction.runPrepare();
               }
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });


      killMember(2);
      assertEquals("Wrong number of caches", 2, caches().size());


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
         transaction.runCommitTx();
      } else {
         tm(1).commit();
      }
      assertEquals("Wrong value for key 'k' in " + cache(0), "v", cache(0).get(k));
      assertEquals("Wrong value for key 'k' in " + cache(1), "v", cache(1).get(k));
      assertNotLocked(k);
   }
}
