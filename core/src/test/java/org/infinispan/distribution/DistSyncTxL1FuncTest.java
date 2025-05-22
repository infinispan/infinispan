package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncTxL1FuncTest")
public class DistSyncTxL1FuncTest extends BaseDistSyncL1Test {
   @Override
   public Object[] factory() {
      return new Object[] {
         new DistSyncTxL1FuncTest().isolationLevel(IsolationLevel.READ_COMMITTED),
         new DistSyncTxL1FuncTest().isolationLevel(IsolationLevel.REPEATABLE_READ)
      };
   }

   public DistSyncTxL1FuncTest() {
      transactional = true;
      testRetVals = true;
   }

   @Override
   protected Class<? extends AsyncInterceptor> getDistributionInterceptorClass() {
      return isVersioned() ? VersionedDistributionInterceptor.class : TxDistributionInterceptor.class;
   }

   @Override
   protected Class<? extends AsyncInterceptor> getL1InterceptorClass() {
      return L1TxInterceptor.class;
   }

   protected Class<? extends VisitableCommand> getCommitCommand() {
      return isVersioned() ? VersionedCommitCommand.class : CommitCommand.class;
   }

   private boolean isVersioned() {
      return (lockingMode == null || lockingMode == LockingMode.OPTIMISTIC) &&
            (isolationLevel == null || isolationLevel == IsolationLevel.REPEATABLE_READ);
   }

   @Override
   protected <K> void assertL1StateOnLocalWrite(Cache<? super K, ?> cache, Cache<?, ?> updatingCache, K key, Object valueWrite) {
      if (cache != updatingCache) {
         super.assertL1StateOnLocalWrite(cache, updatingCache, key, valueWrite);
      }
      else {
         InternalCacheEntry ice = cache.getAdvancedCache().getDataContainer().peek(key);
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
   public void testL1UpdatedBeforePutCommits() throws Exception {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      nonOwnerCache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(firstValue, nonOwnerCache.put(key, secondValue));

      InternalCacheEntry ice = nonOwnerCache.getAdvancedCache().getDataContainer().peek(key);
      assertNotNull(ice);
      assertEquals(firstValue, ice.getValue());
      // Commit the put which should now update
      nonOwnerCache.getAdvancedCache().getTransactionManager().commit();
      ice = nonOwnerCache.getAdvancedCache().getDataContainer().peek(key);
      assertNotNull(ice);
      assertEquals(secondValue, ice.getValue());
   }

   @Test
   public void testL1UpdatedBeforeRemoveCommits() throws Exception {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      assertIsNotInL1(nonOwnerCache, key);

      nonOwnerCache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(firstValue, nonOwnerCache.remove(key));

      InternalCacheEntry ice = nonOwnerCache.getAdvancedCache().getDataContainer().peek(key);
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
         Future<Boolean> futureReplace = fork(() -> nonOwnerCache.replace(key, firstValue, secondValue));

         barrier.await(5, TimeUnit.SECONDS);

         Future<String> futureGet = fork(() -> nonOwnerCache.get(key));
         TestingUtil.assertNotDone(futureGet);

         // Let the replace now finish
         barrier.await(5, TimeUnit.SECONDS);

         assertTrue(futureReplace.get(5, TimeUnit.SECONDS));

         assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   @Test
   public void testGetOccursAfterReplaceRunningBeforeWithRemoteException() throws Exception {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, ReplaceCommand.class, false);

      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(nonOwnerCache);
      try {
         // The replace will internally block the get until it gets the remote value
         Future<Boolean> futureReplace = fork(() -> nonOwnerCache.replace(key, firstValue, secondValue));

         barrier.await(5, TimeUnit.SECONDS);

         Future<String> futureGet = fork(() -> nonOwnerCache.get(key));

         // The get will blocks locally on the L1WriteSynchronizer registered by the replace command
         TestingUtil.assertNotDone(futureGet);
         controlledRpcManager.expectNoCommand();

         // Continue the replace
         barrier.await(5, TimeUnit.SECONDS);

         // That also unblocks the get command and allows it to perform the remote get
         controlledRpcManager.expectCommand(ClusteredGetCommand.class)
                             .skipSend()
                             .receive(address(ownerCache), new ExceptionResponse(new TestException()));

         Exceptions.expectExecutionException(RemoteException.class, TestException.class, futureReplace);

         Exceptions.expectExecutionException(RemoteException.class, TestException.class, futureGet);
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
         controlledRpcManager.revertRpcManager();
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
         Future<String> futureReplace = fork(() -> nonOwnerCache.put(key, secondValue));

         barrier.await(5, TimeUnit.SECONDS);

         Future<String> futureGet = fork(() -> nonOwnerCache.get(key));

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

   /**
    * See ISPN-3648
    */
   public void testBackupOwnerInvalidatesL1WhenPrimaryIsUnaware() throws InterruptedException, TimeoutException,
                                                                      BrokenBarrierException, ExecutionException {

      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> backupOwnerCache = owners[1];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertEquals(firstValue, nonOwnerCache.get(key));

      assertIsInL1(nonOwnerCache, key);

      // Add a barrier to block the commit on the backup owner so it doesn't yet update the value.  Note this
      // will also block the primary owner since it is a sync call
      CyclicBarrier backupPutBarrier = new CyclicBarrier(2);
      addBlockingInterceptor(backupOwnerCache, backupPutBarrier, getCommitCommand(), getL1InterceptorClass(),
                             false);

      try {
         Future<String> future = fork(() -> ownerCache.put(key, secondValue));

         // Wait until owner has tried to replicate to backup owner
         backupPutBarrier.await(10, TimeUnit.SECONDS);

         assertEquals(firstValue, ownerCache.getAdvancedCache().getDataContainer().peek(key).getValue());
         assertEquals(firstValue, backupOwnerCache.getAdvancedCache().getDataContainer().peek(key).getValue());

         // Now remove the interceptor, just so we can add another.  This is okay since it still retains the next
         // interceptor reference properly
         removeAllBlockingInterceptorsFromCache(ownerCache);

         // Add a barrier to block the get from being retrieved on the primary owner
         CyclicBarrier ownerGetBarrier = new CyclicBarrier(2);
         addBlockingInterceptor(ownerCache, ownerGetBarrier, GetCacheEntryCommand.class, getL1InterceptorClass(),
                                false);

         // This should be retrieved from the backup owner
         assertEquals(firstValue, nonOwnerCache.get(key));

         assertIsInL1(nonOwnerCache, key);

         // Just let the owner put and backup puts complete now
         backupPutBarrier.await(10, TimeUnit.SECONDS);

         // Now wait for the owner put to complete which has to happen before the owner gets the get from non owner
         assertEquals(firstValue, future.get(10, TimeUnit.SECONDS));

         // Finally let the get to go to the owner
         ownerGetBarrier.await(10, TimeUnit.SECONDS);
         ownerGetBarrier.await(10, TimeUnit.SECONDS);

         // This is async in the LastChance interceptor
         eventually(() -> !isInL1(nonOwnerCache, key));

         assertEquals(secondValue, ownerCache.getAdvancedCache().getDataContainer().peek(key).getValue());
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);
      }
   }

   /**
    * See ISPN-3518
    */
   public void testInvalidationSynchronous() throws Exception {
      final Cache<Object, String>[] owners = getOwners(key, 2);

      final Cache<Object, String> ownerCache = owners[0];
      final Cache<Object, String> backupOwnerCache = owners[1];
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);

      ownerCache.put(key, firstValue);

      assertEquals(firstValue, nonOwnerCache.get(key));

      assertIsInL1(nonOwnerCache, key);

      ControlledRpcManager crm = ControlledRpcManager.replaceRpcManager(ownerCache);
      ControlledRpcManager crm2 = ControlledRpcManager.replaceRpcManager(backupOwnerCache);

      try {
         Future<String> future = fork(() -> ownerCache.put(key, secondValue));

         if (!onePhaseCommitOptimization) {
            // With 2PC the invalidation is sent before the commit but after the prepare
            crm.expectCommand(PrepareCommand.class).send().receiveAll();
         }

         // Wait for the L1 invalidation commands and block them
         ControlledRpcManager.BlockedRequest blockedInvalidate1 = crm.expectCommand(InvalidateL1Command.class);
         crm2.expectNoCommand(100, TimeUnit.MILLISECONDS);

         try {
            future.get(1, TimeUnit.SECONDS);
            fail("This should have timed out since, they cannot invalidate L1");
         } catch (TimeoutException e) {
            // We should get a timeout exception as the L1 invalidation commands are blocked and it should be sync
            // so the invalidations are completed before the write completes
         }

         // Now we should let the L1 invalidations go through
         blockedInvalidate1.send().receiveAll();

         if (onePhaseCommitOptimization) {
            // With 1PC the invalidation command is sent before the prepare command
            crm.expectCommand(PrepareCommand.class).send().receiveAll();
         } else {
            crm.expectCommand(CommitCommand.class).send().receiveAll();
            crm.expectCommand(TxCompletionNotificationCommand.class).send();
         }

         assertEquals(firstValue, future.get(10, TimeUnit.SECONDS));

         assertIsNotInL1(nonOwnerCache, key);

         assertEquals(secondValue, nonOwnerCache.get(key));
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);

         crm.revertRpcManager();
         crm2.revertRpcManager();
      }
   }
}
