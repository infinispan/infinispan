package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.distribution.L1NonTxInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "distribution.DistSyncL1FuncTest")
public class DistSyncL1FuncTest extends BaseDistSyncL1Test {

   public DistSyncL1FuncTest() {
      testRetVals = true;
   }

   @Override
   protected Class<? extends AsyncInterceptor> getDistributionInterceptorClass() {
      return isTriangle() ? TriangleDistributionInterceptor.class : NonTxDistributionInterceptor.class;
   }

   @Override
   protected Class<? extends AsyncInterceptor> getL1InterceptorClass() {
      return L1NonTxInterceptor.class;
   }

   protected void assertL1PutWithConcurrentUpdate(final Cache<Object, String> nonOwnerCache, Cache<Object, String> ownerCache,
                                                  final boolean replace, final Object key, final String originalValue,
                                                  final String nonOwnerValue, String updateValue) throws Throwable {
      CyclicBarrier barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor = addBlockingInterceptorBeforeTx(nonOwnerCache, barrier,
            replace ? ReplaceCommand.class : PutKeyValueCommand.class);

      try {
         Future<String> future = fork(() -> {
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
         });

         // Now wait for the put/replace to return and block it for now
         barrier.await(5, TimeUnit.SECONDS);

         // Stop blocking new commands as we check that a put returns the correct previous value
         blockingInterceptor.suspend(true);

         // Owner should have the new value
         assertEquals(nonOwnerValue, ownerCache.put(key, updateValue));

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

   public void testNoEntryInL1PutWithConcurrentInvalidation() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   public void testEntryInL1PutWithConcurrentInvalidation() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   public void testNoEntryInL1PutWithConcurrentPut() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   public void testEntryInL1PutWithConcurrentPut() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, false, key, firstValue, "intermediate-put", secondValue);
   }

   public void testNoEntryInL1ReplaceWithConcurrentInvalidation() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   public void testEntryInL1ReplaceWithConcurrentInvalidation() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, ownerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   public void testNoEntryInL1ReplaceWithConcurrentPut() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in the owner, so the L1 is empty
      ownerCache.put(key, firstValue);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   public void testEntryInL1ReplaceWithConcurrentPut() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   public void testNoEntryInL1GetWithConcurrentReplace() throws Throwable {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      // Put the first value in a non owner, so the L1 has the key
      ownerCache.put(key, firstValue);
      nonOwnerCache.get(key);

      assertIsInL1(nonOwnerCache, key);

      assertL1PutWithConcurrentUpdate(nonOwnerCache, nonOwnerCache, true, key, firstValue, "intermediate-put", secondValue);
   }

   public void testNoEntryInL1PutReplacedNullValueConcurrently() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ControlledRpcManager crm = ControlledRpcManager.replaceRpcManager(nonOwnerCache);
      crm.excludeCommands(ClusteredGetCommand.class);

      try {
         Future<String> future = fork(() -> nonOwnerCache.putIfAbsent(key, firstValue));

         // Now wait for the get to return and block it for now
         ControlledRpcManager.BlockedResponseMap blockedPutResponses =
            crm.expectCommand(PutKeyValueCommand.class).send().expectAllResponses();

         // Owner should have the new value
         assertEquals(firstValue, ownerCache.remove(key));

         // Now let owner key->updateValue go through
         blockedPutResponses.receive();

         // This should be originalValue still as we did the get
         assertNull(future.get(5, TimeUnit.SECONDS));

         // Remove the interceptor now since we don't want to block ourselves - if using phaser this isn't required
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);

         assertIsNotInL1(nonOwnerCache, key);
         // The nonOwnerCache should retrieve new value as it isn't in L1
         assertNull(nonOwnerCache.get(key));
         assertIsNotInL1(nonOwnerCache, key);
      } finally {
         crm.revertRpcManager();
      }
   }

   public void testNonOwnerRetrievesValueFromBackupOwnerWhileWrite() throws Exception {
      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> backupOwnerCache = owners[1];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertEquals(firstValue, nonOwnerCache.get(key));

      assertIsInL1(nonOwnerCache, key);

      // Add a barrier to block the owner from receiving the get command from the non owner
      CyclicBarrier ownerGetBarrier = new CyclicBarrier(2);
      addBlockingInterceptor(ownerCache, ownerGetBarrier, GetCacheEntryCommand.class, L1NonTxInterceptor.class, false);

      // Add a barrier to block the backup owner from committing the write to memory
      CyclicBarrier backupOwnerWriteBarrier = new CyclicBarrier(2);
      addBlockingInterceptor(backupOwnerCache, backupOwnerWriteBarrier, PutKeyValueCommand.class, L1NonTxInterceptor.class, true);

      try {
         Future<String> future = fork(() -> ownerCache.put(key, secondValue));

         // Wait until the put is trying to replicate
         backupOwnerWriteBarrier.await(5, TimeUnit.SECONDS);

         // Wait until the L1 is cleared out from the owners L1 invalidation
         eventually(() -> !isInL1(nonOwnerCache, key), 5000, 50, TimeUnit.MILLISECONDS);

         // This should come back from the backup owner, since the primary owner is blocked
         assertEquals(firstValue, nonOwnerCache.get(key));

         assertIsInL1(nonOwnerCache, key);

         // Now let the backup owner put complete and send response
         backupOwnerWriteBarrier.await(5, TimeUnit.SECONDS);

         // Wait for the put to complete
         future.get(5, TimeUnit.SECONDS);

         // The Last chance interceptor is async so wait to make sure it was invalidated
         eventually(() -> !isInL1(nonOwnerCache, key), 5000, 50, TimeUnit.MILLISECONDS);
         // The L1 value shouldn't be present
         assertIsNotInL1(nonOwnerCache, key);

         // Now finally let the get from the non owner to the primary owner go, which at this point will finally
         // register the requestor
         ownerGetBarrier.await(5, TimeUnit.SECONDS);
         ownerGetBarrier.await(5, TimeUnit.SECONDS);

         // The L1 value shouldn't be present
         assertIsNotInL1(nonOwnerCache, key);
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);
      }
   }

   /**
    * See ISPN-3617
    */
   public void testNonOwnerRemovesValueFromL1ProperlyOnWrite() throws InterruptedException, TimeoutException,
                                                                      BrokenBarrierException, ExecutionException {

      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> backupOwnerCache = owners[1];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertEquals(firstValue, nonOwnerCache.get(key));

      assertIsInL1(nonOwnerCache, key);

      // Add a barrier to block the owner from actually updating it's own local value
      CyclicBarrier ownerPutBarrier = new CyclicBarrier(2);
      addBlockingInterceptor(ownerCache, ownerPutBarrier, PutKeyValueCommand.class, L1NonTxInterceptor.class, true);

      // Add a barrier to block the get from being retrieved on the backup owner
      CyclicBarrier backupGetBarrier = new CyclicBarrier(2);
      addBlockingInterceptor(backupOwnerCache, backupGetBarrier, GetCacheEntryCommand.class, L1NonTxInterceptor.class,
                             false);

      try {
         Future<String> future = fork(() -> nonOwnerCache.put(key, secondValue));

         // Wait until owner has already replicated to backup owner, but hasn't updated local value
         ownerPutBarrier.await(10, TimeUnit.SECONDS);

         CommandAckCollector collector = TestingUtil.extractComponent(nonOwnerCache, CommandAckCollector.class);
         List<Long> pendingIds = collector.getPendingCommands();
         assertEquals(1, pendingIds.size());
         eventually(() -> !collector.hasPendingBackupAcks(pendingIds.get(0)));

         assertEquals(firstValue, ownerCache.getAdvancedCache().getDataContainer().get(key).getValue());
         assertEquals(secondValue, backupOwnerCache.getAdvancedCache().getDataContainer().get(key).getValue());

         assertEquals(firstValue, nonOwnerCache.get(key));

         assertIsInL1(nonOwnerCache, key);

         // Let the backup get return now
         try {
            backupGetBarrier.await(5, TimeUnit.SECONDS);
            backupGetBarrier.await(5, TimeUnit.SECONDS);
         } catch (TimeoutException e) {
            // A timeout is expected if the backup never gets the request (because of the staggered get)
         }

         // Finally let the put complete
         ownerPutBarrier.await(10, TimeUnit.SECONDS);

         assertEquals(firstValue, future.get(10, TimeUnit.SECONDS));

         assertIsNotInL1(nonOwnerCache, key);

         assertEquals(secondValue, ownerCache.getAdvancedCache().getDataContainer().get(key).getValue());
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);
      }
   }
}
