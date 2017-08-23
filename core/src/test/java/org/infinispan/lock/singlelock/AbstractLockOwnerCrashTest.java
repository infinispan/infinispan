package org.infinispan.lock.singlelock;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractLockOwnerCrashTest extends AbstractCrashTest {

   public AbstractLockOwnerCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      super(cacheMode, lockingMode, useSynchronization);
   }

   protected EmbeddedTransaction transaction;

   public void testOwnerChangesAfterPrepare1() throws Exception {
      testOwnerChangesAfterPrepare(0);
   }

   public void testOwnerChangesAfterPrepare2() throws Exception {
      testOwnerChangesAfterPrepare(1);
   }

   private void testOwnerChangesAfterPrepare(final int secondTxNode) throws Exception {
      final Object k = getKeyForCache(2);
      fork(() -> {
         try {
            tm(1).begin();
            cache(1).put(k, "v");
            transaction = (EmbeddedTransaction) tm(1).getTransaction();
            log.trace("Before preparing");
            transaction.runPrepare();
            tm(1).suspend();
         } catch (Throwable e) {
            log.errorf(e, "Error preparing transaction for key %s", k);
         }
      });


      Eventually.eventually(() -> checkTxCount(0, 0, 1) &&  checkTxCount(1, 1, 0) &&  checkTxCount(2, 0, 1));

      killMember(2);
      assert caches().size() == 2;


      tm(secondTxNode).begin();
      final Transaction suspend = tm(secondTxNode).suspend();
      fork(() -> {
         try {
            log.trace("This thread runs a different tx");
            tm(secondTxNode).resume(suspend);
            cache(secondTxNode).put(k, "v2");
            tm(secondTxNode).commit();
         } catch (Exception e) {
            log.errorf(e, "Error committing transaction for key %s", k);
         }
      });

      // this 'ensures' transaction called 'suspend' has the chance to start the prepare phase and is waiting to acquire the locks on k held by first transaction before it gets resumed
      Thread.sleep(1000);

      log.trace("Before completing the transaction!");
      tm(1).resume(transaction);
      transaction.runCommit(false);

      //make sure the 2nd transaction succeeds as well eventually
      Eventually.eventually(() -> cache(0).get(k).equals("v2") && cache(1).get(k).equals("v2"), 15000);
      assertNotLocked(k);

      Eventually.eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
   }

}
