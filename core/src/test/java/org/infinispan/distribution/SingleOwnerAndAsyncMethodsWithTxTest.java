package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * Transactional tests for asynchronous methods in a distributed
 * environment and a single owner.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.SingleOwnerAndAsyncMethodsWithTxTest")
public class SingleOwnerAndAsyncMethodsWithTxTest extends BaseDistFunctionalTest<Object, String> {

   public SingleOwnerAndAsyncMethodsWithTxTest() {
      INIT_CLUSTER_SIZE = 2;
      numOwners = 1;
      transactional = true;
      l1CacheEnabled = true;
      lockTimeout = 5;
      lockingMode = LockingMode.PESSIMISTIC;
   }

   public void testAsyncGetsWithinTx(Method m) throws Exception {
      String k = k(m);
      String v = v(m);
      Cache<Object, String> ownerCache = getOwner(k);
      Cache<Object, String> nonOwnerCache = getNonOwner(k);
      ownerCache.put(k, v);

      TransactionManager tm = getTransactionManager(nonOwnerCache);
      tm.begin();
      CompletableFuture<String> f = nonOwnerCache.getAsync(k);
      assertNotNull(f);
      assertEquals(v, f.get());

      CompletableFuture<Map<Object, String>> allF = nonOwnerCache.getAllAsync(Collections.singleton(k));
      assertNotNull(allF);
      assertEquals(Collections.singletonMap(k, v), allF.get());

      nonOwnerCache.put(k, v(m, 2));
      tm.commit();

      f = nonOwnerCache.getAsync(k);
      assertNotNull(f);
      assertEquals(v(m, 2), f.get());

      allF = nonOwnerCache.getAllAsync(Collections.singleton(k));
      assertNotNull(allF);
      assertEquals(Collections.singletonMap(k, v(m, 2)), allF.get());
   }

   public void testAsyncGetToL1AndConcurrentModification(final Method m) throws Throwable {
      // The storage to L1 should fail "silently" and not affect other transactions.
      modifyConcurrently(m, getNonOwner(k(m)), false);
   }

   public void testAsyncGetWithForceWriteLockFlag(final Method m) throws Throwable {
      modifyConcurrently(m, getOwner(k(m)), true);
   }

   private void modifyConcurrently(final Method m, final Cache cache, final boolean withFlag) throws Throwable {
      final String k = k(m);
      final String v = v(m);
      Cache<Object, String> ownerCache = getOwner(k);
      ownerCache.put(k, v);

      final CountDownLatch getAsynclatch = new CountDownLatch(1);
      final CountDownLatch putLatch = new CountDownLatch(1);
      Callable<Void> c1 = () -> {
         Cache localCache = cache;
         TransactionManager tm = getTransactionManager(localCache);
         tm.begin();
         // This brings k,v to L1 in non-owner cache
         if (withFlag)
            localCache = cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK);

         CompletableFuture<String> f = localCache.getAsync(k);
         CompletableFuture<Map<String, String>> allF = localCache.getAllAsync(Collections.singleton(k));
         assertNotNull(f);
         assertEquals(v, f.get());
         assertNotNull(allF);
         assertEquals(Collections.singletonMap(k, v), allF.get());

         putLatch.countDown();
         getAsynclatch.await();
         tm.commit();
         return null;
      };

      Callable<Void> c2 = () -> {
         putLatch.await();
         TransactionManager tm = getTransactionManager(cache);
         tm.begin();
         try {
            // If getAsync was done within a tx, k should be locked
            // and put() should timeout
            cache.put(k, v(m, 1));
            getAsynclatch.countDown();
            assertFalse("Put operation should have timed out if the get operation acquires a write lock", withFlag);
         } catch (TimeoutException e) {
            tm.setRollbackOnly();
            getAsynclatch.countDown();
            throw e;
         } finally {
            if (tm.getStatus() == Status.STATUS_ACTIVE)
               tm.commit();
            else
               tm.rollback();
         }
         return null;
      };

      Future f1 = fork(c1);
      Future f2 = fork(c2);

      f1.get();
      try {
         f2.get();
         assert !withFlag : "Should throw a TimeoutException if the get operation acquired a lock";
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         if (cause instanceof AssertionError)
            throw cause; // Assert failed so rethrow as is
         else
            assert e.getCause() instanceof TimeoutException : String.format(
               "The exception should be a TimeoutException but instead was %s",
               e.getCause());
      }
   }

   protected Cache<Object, String> getOwner(Object key) {
      return getOwners(key)[0];
   }

   protected Cache<Object, String> getNonOwner(Object key) {
      return getNonOwners(key)[0];
   }

   public Cache<Object, String>[] getOwners(Object key) {
      return getOwners(key, 1);
   }

   public Cache<Object, String>[] getNonOwners(Object key) {
      return getNonOwners(key, 1);
   }

}
