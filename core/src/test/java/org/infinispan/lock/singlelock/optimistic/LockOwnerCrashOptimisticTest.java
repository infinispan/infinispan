package org.infinispan.lock.singlelock.optimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;


/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.optimistic.LockOwnerCrashOptimisticTest")
@CleanupAfterMethod
public class LockOwnerCrashOptimisticTest extends AbstractLockOwnerCrashTest {

   public LockOwnerCrashOptimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC, false);
   }

   private DummyTransaction transaction;

   public void testLockOwnerCrashesBeforePrepare() throws Exception {
      final Object k = getKeyForCache(2);
      inNewThread(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
            } catch (Throwable e) {
               log.errorf(e, "Error starting transaction for key %s", k);
            }
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 1, 0)&& checkTxCount(2, 0, 0);
         }
      });

      killMember(2);
      assert caches().size() == 2;

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("v", cache(0).get(k));
      assertEquals("v", cache(1).get(k));

      assertNotLocked(k);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit() throws Exception {
      final Object k = getKeyForCache(2);
      inNewThread(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               transaction.runPrepare();
            } catch (Throwable e) {
               log.errorf(e, "Error preparing transaction for key %s", k);
            }
         }
      });


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) &&  checkTxCount(1, 1, 0) &&  checkTxCount(2, 0, 1);
         }
      });

      killMember(2);
      assert caches().size() == 2;


      tm(1).begin();
      cache(1).put(k, "v3");
      try {
         tm(1).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         log.debugf(e, "Expected error committing transaction for key %s", k);
      }


      tm(0).begin();
      cache(0).put(k, "v2");
      try {
         tm(0).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         log.debugf(e, "Expected error committing transaction for key %s", k);
      }

      tm(1).resume(transaction);
      transaction.runPrepare();
   }
}
