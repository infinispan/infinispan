package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Test(groups = "functional", testName = "distribution.DistSyncL1FuncTest")
public class DistSyncL1FuncTest extends BaseDistFunctionalTest {

   public DistSyncL1FuncTest() {
      sync = true;
      tx = false;
      testRetVals = true;
   }

   protected final static String key = "key-to-the-cache";
   protected static final String firstValue = "first-put";
   protected static final String secondValue = "second-put";

   private void addBlockingInterceptorBeforeTx(Cache<?, ?> cache, final CyclicBarrier barrier,
                                               Class<? extends VisitableCommand> commandClass) {
      addBlockingInterceptorBeforeTx(cache, barrier, commandClass, true);
   }

   private void addBlockingInterceptorBeforeTx(Cache<?, ?> cache, final CyclicBarrier barrier,
                                                Class<? extends VisitableCommand> commandClass, boolean blockAfter) {
      cache.getAdvancedCache().addInterceptorBefore(new BlockingInterceptor(barrier, commandClass, blockAfter), NonTxDistributionInterceptor.class);
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

         assertIsNotInL1(nonOwnerCache, key);
         // The nonOwnerCache should retrieve new value as it isn't in L1
         assertEquals(updateValue, nonOwnerCache.get(key));
         assertIsInL1(nonOwnerCache, key);
      }
      finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   protected void assertL1PutWithConcurrentUpdate(final Cache<Object, String> nonOwnerCache, Cache<Object, String> ownerCache,
                                                  final boolean replace, final Object key, final String originalValue,
                                                  final String nonOwnerValue, String updateValue) throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, replace ? ReplaceCommand.class : PutKeyValueCommand.class);

      try {
         Future<String> future = fork(new Callable<String>() {
            @Override
            public String call() throws Exception {
               if (replace) {
                  // This should always be true
                  if (nonOwnerCache.replace(key, originalValue, nonOwnerValue)) {
                     return originalValue;
                  }
                  return nonOwnerCache.get(key);
               }
               else {
                  return nonOwnerCache.put(key, nonOwnerValue);
               }
            }
         });

         // Now wait for the get to return and block it for now
         barrier.await(5, TimeUnit.SECONDS);

         // Owner should have the new value
         assertEquals(nonOwnerValue, ownerCache.put(key, updateValue));

         // Now let owner key->updateValue go through
         barrier.await(5, TimeUnit.SECONDS);

         // This should be originalValue still as we did the get
         assertEquals(originalValue, future.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         assertIsNotInL1(nonOwnerCache, key);
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
   public void testNoEntryInL1GetWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1GetWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, key, firstValue, secondValue);
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
   public void testNoEntryInL1PutWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testEntryInL1PutWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testNoEntryInL1PutWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testEntryInL1PutWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testNoEntryInL1ReplaceWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testEntryInL1ReplaceWithConcurrentInvalidation() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testNoEntryInL1ReplaceWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testEntryInL1ReplaceWithConcurrentPut() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testNoEntryInL1GetWithConcurrentRemove() throws InterruptedException, ExecutionException, TimeoutException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   @Test
   public void testNoEntryInL1PutReplacedNullValueConcurrently() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      RpcManager rm = TestingUtil.extractComponent(nonOwnerCache, RpcManager.class);
      ControlledRpcManager crm = new ControlledRpcManager(rm);
      // Make our node block and not return the get yet
      crm.blockAfter(PutKeyValueCommand.class);
      TestingUtil.replaceComponent(nonOwnerCache, RpcManager.class, crm, true);

      try {
         Future<String> future = nonOwnerCache.putIfAbsentAsync(key, firstValue);

         // Now wait for the get to return and block it for now
         crm.waitForCommandToBlock(5, TimeUnit.SECONDS);

         // Owner should have the new value
         assertEquals(firstValue, ownerCache.remove(key));

         // Now let owner key->updateValue go through
         crm.stopBlocking();

         // This should be originalValue still as we did the get
         assertNull(future.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         assertIsNotInL1(nonOwnerCache, key);
         // The nonOwnerCache should retrieve new value as it isn't in L1
         assertNull(nonOwnerCache.get(key));
         assertIsNotInL1(nonOwnerCache, key);
      } finally {
         TestingUtil.replaceComponent(nonOwnerCache, RpcManager.class, rm, true);
      }
   }

   // TODO: this test fails beacuse ISPN-2965 needs to be fixed to send another invalidation if a requestor came
   // while the invalidation was still being processed - once it is fixed this should work when enabled
   @Test(enabled = false, description = "This test is disabled as it will always fail until ISPN-2965 is fixed")
   public void testNoEntryInL1MultipleConcurrentGetsWithInvalidation() throws TimeoutException, InterruptedException, ExecutionException, BrokenBarrierException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier invalidationBarrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, invalidationBarrier, InvalidateL1Command.class);

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

         // This should be originalValue still as we did the get before the invalidation completed
         assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         // The value shouldn't be in the L1 still
         assertIsNotInL1(nonOwnerCache, key);
         // The nonOwnerCache should retrieve new value as it isn't in L1
         assertEquals(secondValue, nonOwnerCache.get(key));
         assertIsInL1(nonOwnerCache, key);
      }
      finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }
}
