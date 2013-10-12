package org.infinispan.distribution;

import org.apache.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Base class for various L1 tests for use with distributed cache.  Note these only currently work for synchronous based
 * caches
 *
 * @author wburns
 * @since 6.0
 */
public abstract class BaseDistSyncL1Test extends BaseDistFunctionalTest<Object, String> {

   protected static final String key = "key-to-the-cache";
   protected static final String firstValue = "first-put";
   protected static final String secondValue = "second-put";

   protected void addBlockingInterceptorBeforeTx(Cache<?, ?> cache, final CyclicBarrier barrier,
                                                 Class<? extends VisitableCommand> commandClass) {
      addBlockingInterceptorBeforeTx(cache, barrier, commandClass, true);
   }

   protected void addBlockingInterceptorBeforeTx(Cache<?, ?> cache, final CyclicBarrier barrier,
                                                 Class<? extends VisitableCommand> commandClass,
                                                 boolean blockAfterCommand) {
      addBlockingInterceptor(cache, barrier, commandClass, getDistributionInterceptorClass(), blockAfterCommand);
   }

   protected void addBlockingInterceptor(Cache<?, ?> cache, final CyclicBarrier barrier,
                                         Class<? extends VisitableCommand> commandClass,
                                         Class<? extends CommandInterceptor> interceptorPosition,
                                         boolean blockAfterCommand) {
      cache.getAdvancedCache().addInterceptorBefore(new BlockingInterceptor(barrier, commandClass, blockAfterCommand),
                                                    interceptorPosition);
   }

   protected abstract Class<? extends CommandInterceptor> getDistributionInterceptorClass();

   protected abstract Class<? extends CommandInterceptor> getL1InterceptorClass();

   protected void assertL1StateOnLocalWrite(Cache<?,?> cache, Cache<?, ?> updatingCache, Object key, Object valueWrite) {
      // Default just assumes it invalidated the cache
      assertIsNotInL1(cache, key);
   }

   protected void assertL1GetWithConcurrentUpdate(final Cache<Object, String> nonOwnerCache, Cache<Object, String> ownerCache,
                                                  final Object key, String originalValue, String updateValue)
         throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, GetKeyValueCommand.class);

      try {
         Future<String> future = nonOwnerCache.getAsync(key);

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

   @Test()
   public void testNoEntryInL1MultipleConcurrentGetsWithInvalidation() throws TimeoutException, InterruptedException, ExecutionException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier invalidationBarrier = new CyclicBarrier(2);
      // We want to block right before the invalidation would hit the L1 interceptor to prevent it from invaliding until we want
      nonOwnerCache.getAdvancedCache().addInterceptorBefore(
            new BlockingInterceptor(invalidationBarrier, InvalidateL1Command.class, false), getL1InterceptorClass());

      try {
         assertEquals(firstValue, nonOwnerCache.get(key));

         Future<String> futurePut = ownerCache.putAsync(key, secondValue);

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

         Future<String> futureGet = nonOwnerCache.getAsync(key);

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
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               // The nonOwnerCache should retrieve new value as it isn't in L1
               assertEquals(secondValue, nonOwnerCache.get(key));
               return isInL1(nonOwnerCache, key);
            }
         });
      }
      finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }
}
