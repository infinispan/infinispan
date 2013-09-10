package org.infinispan.distribution;

import org.infinispan.Cache;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test that emulates transactions being started in a thread and then being
 * committed in a different thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.DistSyncTxCommitDiffThreadTest")
public class DistSyncTxCommitDiffThreadTest extends BaseDistFunctionalTest<Object, String> {

   public DistSyncTxCommitDiffThreadTest() {
      cacheName = this.getClass().getSimpleName();
      INIT_CLUSTER_SIZE = 2;
      sync = true;
      tx = true;
      l1CacheEnabled = false;
      numOwners = 1;
   }

   public void testCommitInDifferentThread(Method m) throws Exception {
      final String key = k(m), value = v(m);
      final Cache nonOwnerCache = getNonOwners(key, 1)[0];
      final Cache ownerCache = getOwners(key, 1)[0];
      final TransactionManager tmNonOwner = getTransactionManager(nonOwnerCache);
      final CountDownLatch commitLatch = new CountDownLatch(1);

      tmNonOwner.begin();
      final Transaction tx = tmNonOwner.getTransaction();
      Callable<Void> commitCallable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            tmNonOwner.resume(tx);
            commitLatch.await();
            tmNonOwner.commit();
            return null;
         }
      };
      Future commitFuture = fork(commitCallable);
      Thread.sleep(500);
      nonOwnerCache.put(key, value);
      commitLatch.countDown();
      commitFuture.get();

      Callable<Void> getCallable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            TransactionManager tmOwner = getTransactionManager(ownerCache);
            tmOwner.begin();
            assertEquals(value, ownerCache.get(key));
            tmOwner.commit();
            return null;
         }
      };

      Future getFuture = fork(getCallable);
      getFuture.get();
   }

}
