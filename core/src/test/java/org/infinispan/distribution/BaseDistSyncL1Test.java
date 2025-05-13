package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.distribution.L1WriteSynchronizer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.TransactionMode;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Base class for various L1 tests for use with distributed cache.  Note these only currently work for synchronous based
 * caches
 *
 * @author wburns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.BaseDistSyncL1Test")
public abstract class BaseDistSyncL1Test extends BaseDistFunctionalTest<Object, String> {

   protected static final String key = "key-to-the-cache";
   protected static final String firstValue = "first-put";
   protected static final String secondValue = "second-put";

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.locking().isolationLevel(isolationLevel);
      return builder;
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   protected BlockingInterceptor addBlockingInterceptorBeforeTx(Cache<?, ?> cache,
                                                                final CyclicBarrier barrier,
                                                                Class<? extends VisitableCommand> commandClass) {
      return addBlockingInterceptorBeforeTx(cache, barrier, commandClass, true);
   }

   protected BlockingInterceptor addBlockingInterceptorBeforeTx(Cache<?, ?> cache, final CyclicBarrier barrier,
                                                                Class<? extends VisitableCommand> commandClass,
                                                                boolean blockAfterCommand) {
      return addBlockingInterceptor(cache, barrier, commandClass, getDistributionInterceptorClass(),
            blockAfterCommand);
   }

   protected BlockingInterceptor addBlockingInterceptor(Cache<?, ?> cache, final CyclicBarrier barrier,
                                                        Class<? extends VisitableCommand> commandClass,
                                                        Class<? extends AsyncInterceptor> interceptorPosition,
                                                        boolean blockAfterCommand) {
      BlockingInterceptor bi = new BlockingInterceptor<>(barrier, commandClass, blockAfterCommand, false);
      AsyncInterceptorChain interceptorChain = extractInterceptorChain(cache);
      assertTrue(interceptorChain.addInterceptorBefore(bi, interceptorPosition));
      return bi;
   }

   protected abstract Class<? extends AsyncInterceptor> getDistributionInterceptorClass();

   protected abstract Class<? extends AsyncInterceptor> getL1InterceptorClass();

   protected <K> void assertL1StateOnLocalWrite(Cache<? super K,?> cache, Cache<?, ?> updatingCache, K key, Object valueWrite) {
      // Default just assumes it invalidated the cache
      assertIsNotInL1(cache, key);
   }

   protected void assertL1GetWithConcurrentUpdate(final Cache<Object, String> nonOwnerCache, Cache<Object, String> ownerCache,
                                                  final Object key, String originalValue, String updateValue)
         throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, GetKeyValueCommand.class);

      try {
         Future<String> future = fork(() -> nonOwnerCache.get(key));

         // Now wait for the get to return and block it for now
         barrier.await(5, TimeUnit.SECONDS);

         assertEquals(originalValue, ownerCache.put(key, updateValue));

         // Now let owner key->updateValue go through
         barrier.await(5, TimeUnit.SECONDS);

         // This should be originalValue still as we did the get
         assertEquals(originalValue, future.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         assertL1StateOnLocalWrite(nonOwnerCache, ownerCache, key, updateValue);
         // The nonOwnerCache should retrieve new value as it isn't in L1
         assertEquals(updateValue, nonOwnerCache.get(key));
         assertIsInL1(nonOwnerCache, key);
      }
      finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   @Test
   public void testNoEntryInL1GetWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1GetWithConcurrentUpdate(nonOwnerCache, ownerCache, key, firstValue, secondValue);
   }

   @Test
   public void testEntryInL1GetWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1GetWithConcurrentUpdate(nonOwnerCache, ownerCache, key, firstValue, secondValue);
   }

   @Test
   public void testEntryInL1GetWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1GetWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, key, firstValue, secondValue);
   }

   @Test
   public void testNoEntryInL1GetWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1GetWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, key, firstValue, secondValue);
   }

   @Test
   public void testNoEntryInL1MultipleConcurrentGetsWithInvalidation() throws TimeoutException, InterruptedException, ExecutionException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier invalidationBarrier = new CyclicBarrier(2);
      // We want to block right before the invalidation would hit the L1 interceptor to prevent it from invaliding until we want
      addBlockingInterceptor(nonOwnerCache, invalidationBarrier, InvalidateL1Command.class, getL1InterceptorClass(), false);

      try {
         assertEquals(firstValue, nonOwnerCache.get(key));

         Future<String> futurePut = fork(() -> ownerCache.put(key, secondValue));

         // Wait for the invalidation to be processing
         invalidationBarrier.await(5, TimeUnit.SECONDS);

         // Now remove the value - assimilates that a get came earlier to owner registering as a invalidatee, however
         // the invalidation blocked the update from going through
         nonOwnerCache.getAdvancedCache().getDataContainer().remove(key);

         // Hack, but we remove the blocking interceptor while a call is in it, it still retains a reference to the next
         // interceptor to invoke and when we unblock it will continue forward
         // This is done because we can't have 2 interceptors of the same class.
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         CyclicBarrier getBarrier = new CyclicBarrier(2);
         addBlockingInterceptorBeforeTx(nonOwnerCache, getBarrier, GetKeyValueCommand.class);

         Future<String> futureGet = fork(() -> nonOwnerCache.get(key));

         // Wait for the get to retrieve the remote value but not try to update L1 yet
         getBarrier.await(5, TimeUnit.SECONDS);

         // Let the invalidation unblock now
         invalidationBarrier.await(5, TimeUnit.SECONDS);

         // Wait for the invalidation complete fully
         assertEquals(firstValue, futurePut.get(5, TimeUnit.SECONDS));

         // Now let our get go through
         getBarrier.await(5, TimeUnit.SECONDS);

         // Technically this could be firstValue or secondValue depending on the ordering of if the put has updated
         // it's in memory contents (since the L1 is sent asynchronously with the update) - For Tx this is always
         // firstValue - the point though is to ensure it doesn't write to the L1
         assertNotNull(futureGet.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         // The value shouldn't be in the L1 still
         assertIsNotInL1(nonOwnerCache, key);

         // It is possible that the async L1LastChance will blow away this get, so we have to make sure to check
         // it eventually
         eventually(() -> {
            // The nonOwnerCache should retrieve new value as it isn't in L1
            assertEquals(secondValue, nonOwnerCache.get(key));
            return isInL1(nonOwnerCache, key);
         });
      }
      finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   /**
    * See ISPN-3657
    */
   @Test
   public void testGetAfterWriteAlreadyInvalidatedCurrentGet() throws InterruptedException, TimeoutException,
                                                                      BrokenBarrierException, ExecutionException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier nonOwnerGetBarrier = new CyclicBarrier(2);
      // We want to block after it retrieves the value from remote owner so the L1 value will be invalidated
      BlockingInterceptor blockingInterceptor =
            addBlockingInterceptor(nonOwnerCache, nonOwnerGetBarrier, GetKeyValueCommand.class,
                  getDistributionInterceptorClass(), true);

      try {

         Future<String> future = fork(() -> nonOwnerCache.get(key));

         // Wait for the get to register L1 before it has sent remote
         nonOwnerGetBarrier.await(10, TimeUnit.SECONDS);
         blockingInterceptor.suspend(true);

         // Now force the L1 sync to be blown away by an update
         ownerCache.put(key, secondValue);

         assertEquals(secondValue, nonOwnerCache.get(key));

         // It should be in L1 now with the second value
         assertIsInL1(nonOwnerCache, key);
         assertEquals(secondValue, nonOwnerCache.getAdvancedCache().getDataContainer().peek(key).getValue());

         // Now let the original get complete
         nonOwnerGetBarrier.await(10, TimeUnit.SECONDS);

         assertEquals(firstValue, future.get(10, TimeUnit.SECONDS));

         // It should STILL be in L1 now with the second value
         assertIsInL1(nonOwnerCache, key);
         assertEquals(secondValue, nonOwnerCache.getAdvancedCache().getDataContainer().peek(key).getValue());

      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   /**
    * See ISPN-3364
    */
   @Test
   public void testRemoteGetArrivesButWriteOccursBeforeRegistration() throws Throwable {
      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> backupOwnerCache = owners[1];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      // Add a barrier to block the owner/backupowner from going further after retrieving the value before coming back into the L1
      // interceptor
      CyclicBarrier getBarrier = new CyclicBarrier(3);
      addBlockingInterceptor(ownerCache, getBarrier, GetCacheEntryCommand.class,
            getL1InterceptorClass(), true);
      addBlockingInterceptor(backupOwnerCache, getBarrier, GetCacheEntryCommand.class,
            getL1InterceptorClass(), true);

      try {
         Future<String> future = fork(() -> nonOwnerCache.get(key));

         // Wait until get goes remote and retrieves value before going back into L1 interceptor
         getBarrier.await(10, TimeUnit.SECONDS);

         assertEquals(firstValue, ownerCache.put(key, secondValue));

         // Let the get complete finally
         getBarrier.await(10, TimeUnit.SECONDS);

         final String expectedValue;
         expectedValue = firstValue;
         assertEquals(expectedValue, future.get(10, TimeUnit.SECONDS));

         assertIsNotInL1(nonOwnerCache, key);
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);
      }
   }

   @Test
   public void testGetBlockedInvalidation() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilAboutToAcquireLock(nonOwnerCache, checkPoint);

      log.warn("Doing get here - ignore all previous");

      Future<String> getFuture = fork(() -> nonOwnerCache.get(key));

      // Wait until we are about to write value into data container on non owner
      checkPoint.awaitStrict("pre_acquire_shared_topology_lock_invoked", 10, TimeUnit.SECONDS);

      Future<String> putFuture = fork(() -> ownerCache.put(key, secondValue));

      Exceptions.expectException(TimeoutException.class, () -> putFuture.get(1, TimeUnit.SECONDS));

      // Let the get complete finally
      checkPoint.triggerForever("pre_acquire_shared_topology_lock_released");

      assertEquals(firstValue, getFuture.get(10, TimeUnit.SECONDS));

      assertEquals(firstValue, putFuture.get(10, TimeUnit.SECONDS));

      assertIsNotInL1(nonOwnerCache, key);
   }

   @Test
   public void testGetBlockingAnotherGet() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      CheckPoint checkPoint = new CheckPoint();
      StateTransferLock lock = waitUntilAboutToAcquireLock(nonOwnerCache, checkPoint);

      try {
         log.warn("Doing get here - ignore all previous");

         Future<String> getFuture = fork(() -> nonOwnerCache.get(key));

         // Wait until we are about to write value into data container on non owner
         checkPoint.awaitStrict("pre_acquire_shared_topology_lock_invoked", 10, TimeUnit.SECONDS);

         Future<String> getFuture2 = fork(() -> nonOwnerCache.get(key));

         Exceptions.expectException(TimeoutException.class, () -> getFuture2.get(1, TimeUnit.SECONDS));

         // Let the get complete finally
         checkPoint.triggerForever("pre_acquire_shared_topology_lock_released");

         assertEquals(firstValue, getFuture.get(10, TimeUnit.SECONDS));

         assertEquals(firstValue, getFuture2.get(10, TimeUnit.SECONDS));

         assertIsInL1(nonOwnerCache, key);
      } finally {
         TestingUtil.replaceComponent(nonOwnerCache, StateTransferLock.class, lock, true);
      }
   }

   @Test
   public void testGetBlockingAnotherGetWithMiss() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      assertIsNotInL1(nonOwnerCache, key);

      CheckPoint checkPoint = new CheckPoint();
      L1Manager l1Manager = waitUntilL1Registration(nonOwnerCache, checkPoint);

      try {
         log.warn("Doing get here - ignore all previous");

         Future<String> getFuture = fork(() -> nonOwnerCache.get(key));

         // Wait until we are about to write value into data container on non owner
         checkPoint.awaitStrict("pre_acquire_shared_topology_lock_invoked", 10, TimeUnit.SECONDS);

         Future<String> getFuture2 = fork(() -> nonOwnerCache.get(key));

         Exceptions.expectException(TimeoutException.class, () -> getFuture2.get(1, TimeUnit.SECONDS));

         // Let the get complete finally
         checkPoint.triggerForever("pre_acquire_shared_topology_lock_released");

         assertNull(getFuture.get(10, TimeUnit.SECONDS));

         assertNull(getFuture2.get(10, TimeUnit.SECONDS));
      } finally {
         TestingUtil.replaceComponent(nonOwnerCache, L1Manager.class, l1Manager, true);
      }
   }

   @Test
   public void testGetBlockingLocalPut() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilAboutToAcquireLock(nonOwnerCache, checkPoint);

      log.warn("Doing get here - ignore all previous");

      Future<String> getFuture = fork(() -> nonOwnerCache.get(key));

      // Wait until we are about to write value into data container on non owner
      checkPoint.awaitStrict("pre_acquire_shared_topology_lock_invoked", 10, TimeUnit.SECONDS);

      Future<String> putFuture = fork(() -> nonOwnerCache.put(key, secondValue));

      Exceptions.expectException(TimeoutException.class, () -> putFuture.get(1, TimeUnit.SECONDS));

      // Let the get complete finally
      checkPoint.triggerForever("pre_acquire_shared_topology_lock_released");

      assertEquals(firstValue, getFuture.get(10, TimeUnit.SECONDS));

      assertEquals(firstValue, putFuture.get(10, TimeUnit.SECONDS));

      if (nonOwnerCache.getCacheConfiguration().transaction().transactionMode() == TransactionMode.TRANSACTIONAL) {
         assertIsInL1(nonOwnerCache, key);
      } else {
         assertIsNotInL1(nonOwnerCache, key);
      }
   }

   public void testL1GetAndCacheEntryGet() {
      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertEquals(firstValue, nonOwnerCache.get(key));

      assertIsInL1(nonOwnerCache, key);

      CacheEntry<Object, String> entry = nonOwnerCache.getAdvancedCache().getCacheEntry(key);
      assertEquals(key, entry.getKey());
      assertEquals(firstValue, entry.getValue());
   }

   @Test
   public void testGetBlockingAnotherGetCacheEntry() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      CheckPoint checkPoint = new CheckPoint();
      StateTransferLock lock = waitUntilAboutToAcquireLock(nonOwnerCache, checkPoint);

      try {
         log.warn("Doing get here - ignore all previous");

         Future<String> getFuture = fork(() -> nonOwnerCache.get(key));

         // Wait until we are about to write value into data container on non owner
         checkPoint.awaitStrict("pre_acquire_shared_topology_lock_invoked", 10, TimeUnit.SECONDS);

         Future<CacheEntry<Object, String>> getFuture2 = fork(() -> nonOwnerCache.getAdvancedCache().getCacheEntry(key));

         Exceptions.expectException(TimeoutException.class, () -> getFuture2.get(1, TimeUnit.SECONDS));

         // Let the get complete finally
         checkPoint.triggerForever("pre_acquire_shared_topology_lock_released");

         assertEquals(firstValue, getFuture.get(10, TimeUnit.SECONDS));

         CacheEntry<Object, String> entry = getFuture2.get(10, TimeUnit.SECONDS);
         assertEquals(key, entry.getKey());
         assertEquals(firstValue, entry.getValue());

         assertIsInL1(nonOwnerCache, key);
      } finally {
         TestingUtil.replaceComponent(nonOwnerCache, StateTransferLock.class, lock, true);
      }
   }

   /**
    * Replaces StateTransferLock in cache with a proxy one that will block on
    * {#link StateTransferLock#acquireSharedTopologyLock} until the checkpoint is triggered
    * @param cache The cache to replace the StateTransferLock on
    * @param checkPoint The checkpoint to use to trigger blocking
    * @return The original real StateTransferLock
    */
   protected StateTransferLock waitUntilAboutToAcquireLock(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateTransferLock stl = TestingUtil.extractComponent(cache, StateTransferLock.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(stl);
      StateTransferLock mockLock = mock(StateTransferLock.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_acquire_shared_topology_lock_invoked");
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_acquire_shared_topology_lock_released", 10, TimeUnit.SECONDS);

         return forwardedAnswer.answer(invocation);
      }).when(mockLock).acquireSharedTopologyLock();
      TestingUtil.replaceComponent(cache, StateTransferLock.class, mockLock, true);
      return stl;
   }

   /**
    * Replaces L1Manager in cache with a proxy one that will block on
    * {#link L1Manager#registerL1WriteSynchronizer} until the checkpoint is triggered
    * @param cache The cache to replace the L1Manager on
    * @param checkPoint The checkpoint to use to trigger blocking
    * @return The original real L1Manager
    */
   protected L1Manager waitUntilL1Registration(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      L1Manager l1Manager = TestingUtil.extractComponent(cache, L1Manager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(l1Manager);
      L1Manager mockL1 = mock(L1Manager.class, withSettings().defaultAnswer(forwardedAnswer).extraInterfaces(RemoteValueRetrievedListener.class));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_acquire_shared_topology_lock_invoked");
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_acquire_shared_topology_lock_released", 10, TimeUnit.SECONDS);

         return forwardedAnswer.answer(invocation);
      }).when(mockL1).registerL1WriteSynchronizer(Mockito.notNull(), Mockito.any(L1WriteSynchronizer.class));
      TestingUtil.replaceComponent(cache, L1Manager.class, mockL1, true);
      return l1Manager;
   }
}
