package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.Transaction;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractLockOwnerCrashTest extends AbstractCrashTest {

   public AbstractLockOwnerCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      super(cacheMode, lockingMode, useSynchronization);
   }

   protected DummyTransaction transaction;

   public void testOwnerChangesAfterPrepare1() throws Exception {
      testOwnerChangesAfterPrepare(0);
   }

   public void testOwnerChangesAfterPrepare2() throws Exception {
      testOwnerChangesAfterPrepare(1);
   }

   private void testOwnerChangesAfterPrepare(final int secondTxNode) throws Exception {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               log.trace("Before preparing");
               transaction.notifyBeforeCompletion();
               transaction.runPrepare();
               tm(1).suspend();
            } catch (Throwable e) {
               e.printStackTrace();
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


      tm(secondTxNode).begin();
      final Transaction suspend = tm(secondTxNode).suspend();
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               log.trace("This thread runs a different tx");
               tm(secondTxNode).resume(suspend);
               cache(secondTxNode).put(k, "v2");
               tm(secondTxNode).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      });

      // this 'ensures' transaction called 'suspend' has the chance to start the prepare phase and is waiting to acquire the locks on k held by first transaction before it gets resumed
      Thread.sleep(1000);

      log.trace("Before completing the transaction!");
      tm(1).resume(transaction);
      transaction.runCommitTx();
      transaction.notifyAfterCompletion(Status.STATUS_COMMITTED);
      tm(1).suspend();

      //make sure the 2nd transaction succeeds as well eventually
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache(0).get(k).equals("v2") && cache(1).get(k).equals("v2");
         }
      }, 15000, 15);
      assertNotLocked(k);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

}
