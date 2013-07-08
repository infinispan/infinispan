package org.infinispan.lock.singlelock.optimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

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
            return checkTxCount(0, 0, 0) && checkTxCount(1, 1, 0)&& checkTxCount(2, 0, 0);
         }
      });

      killMember(2);
      assertEquals("Wrong number of caches", 2, caches().size());

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals("Wrong value for key 'k' in " + cache(0), "v", cache(0).get(k));
      assertEquals("Wrong value for key 'k' in " + cache(1), "v", cache(1).get(k));

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
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               transaction.runPrepare();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) &&  checkTxCount(1, 1, 0) &&  checkTxCount(2, 0, 1);
         }
      });

      killMember(2);
      assertEquals("Wrong number of caches", 2, caches().size());


      tm(1).begin();
      cache(1).put(k, "v3");
      try {
         tm(1).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         e.printStackTrace();
      }


      tm(0).begin();
      cache(0).put(k, "v2");
      try {
         tm(0).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         //expected
      }

      tm(1).resume(transaction);
      transaction.runPrepare();
   }
}
