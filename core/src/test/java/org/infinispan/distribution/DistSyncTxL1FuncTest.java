package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

@Test(groups = "functional", testName = "distribution.DistSyncTxL1FuncTest")
public class DistSyncTxL1FuncTest extends BaseDistSyncL1Test {
   public DistSyncTxL1FuncTest() {
      sync = true;
      tx = true;
      testRetVals = true;
   }

   @Override
   protected Class<? extends CommandInterceptor> getDistributionInterceptorClass() {
      return TxDistributionInterceptor.class;
   }

   @Override
   protected Class<? extends CommandInterceptor> getL1InterceptorClass() {
      return L1TxInterceptor.class;
   }

   @Override
   protected void assertL1StateOnLocalWrite(Cache<?, ?> cache, Cache<?, ?> updatingCache, Object key, Object valueWrite) {
      if (cache != updatingCache) {
         super.assertL1StateOnLocalWrite(cache, updatingCache, key, valueWrite);
      }
      else {
         InternalCacheEntry ice = cache.getAdvancedCache().getDataContainer().get(key);
         assertNotNull(ice);
         assertEquals(valueWrite, ice.getValue());
      }
   }

   @Test
   public void testL1UpdatedOnReplaceOperationFailure() {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      assertFalse(nonOwnerCache.replace(key, "not-same", secondValue));

      assertIsInL1(nonOwnerCache, key);
   }

   @Test
   public void testL1UpdatedOnRemoveOperationFailure() {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      assertFalse(nonOwnerCache.remove(key, "not-same"));

      assertIsInL1(nonOwnerCache, key);
   }

   @Test
   public void testL1UpdatedBeforePutCommits() throws InterruptedException, TimeoutException, BrokenBarrierException,
                                                        ExecutionException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      nonOwnerCache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(firstValue, nonOwnerCache.put(key, secondValue));

      InternalCacheEntry ice = nonOwnerCache.getAdvancedCache().getDataContainer().get(key);
      assertNotNull(ice);
      assertEquals(firstValue, ice.getValue());
      // Commit the put which should now update
      nonOwnerCache.getAdvancedCache().getTransactionManager().commit();
      ice = nonOwnerCache.getAdvancedCache().getDataContainer().get(key);
      assertNotNull(ice);
      assertEquals(secondValue, ice.getValue());
   }

   @Test
   public void testL1UpdatedBeforeRemoveCommits() throws InterruptedException, TimeoutException, BrokenBarrierException,
                                                      ExecutionException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      nonOwnerCache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(firstValue, nonOwnerCache.remove(key));

      InternalCacheEntry ice = nonOwnerCache.getAdvancedCache().getDataContainer().get(key);
      assertNotNull(ice);
      assertEquals(firstValue, ice.getValue());
      // Commit the put which should now update
      nonOwnerCache.getAdvancedCache().getTransactionManager().commit();
      assertIsNotInL1(nonOwnerCache, key);
   }

   @Test
   public void testGetOccursAfterReplaceRunningBeforeRetrievedRemote() throws ExecutionException, InterruptedException, BrokenBarrierException, TimeoutException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, ReplaceCommand.class, false);
      try {
         // The replace will internally block the get until it gets the remote value
         Future<Boolean> futureReplace = nonOwnerCache.replaceAsync(key, firstValue, secondValue);

         barrier.await(5, TimeUnit.SECONDS);

         Future<String> futureGet = nonOwnerCache.getAsync(key);

         try {
            futureGet.get(100, TimeUnit.MILLISECONDS);
            fail("Get shouldn't return until after the replace completes");
         } catch (TimeoutException e) {

         }

         // Let the replace now finish
         barrier.await(5, TimeUnit.SECONDS);

         assertTrue(futureReplace.get());

         assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   @Test
   public void testGetOccursBeforePutCompletesButRetrievesRemote() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier barrier = new CyclicBarrier(2);
      // This way the put should retrieve remote value, but before it has actually tried to update the value
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, PutKeyValueCommand.class, true);
      try {
         // The replace will internally block the get until it gets the remote value
         Future<String> futureReplace = nonOwnerCache.putAsync(key, secondValue);

         barrier.await(5, TimeUnit.SECONDS);

         Future<String> futureGet = nonOwnerCache.getAsync(key);

         // If this errors here it means the get was blocked by the write operation even though it already retrieved
         // the remoteValue and should have unblocked any other waiters
         assertEquals(firstValue, futureGet.get(3, TimeUnit.SECONDS));
         // Just make sure it was put into L1 properly as well
         assertIsInL1(nonOwnerCache, key);

         // Let the put now finish
         barrier.await(5, TimeUnit.SECONDS);

         assertEquals(firstValue, futureReplace.get());

         assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }
}
