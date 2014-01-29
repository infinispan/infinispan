package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.distribution.L1NonTxInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testng.AssertJUnit.*;

import static org.mockito.Mockito.*;

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

   protected Class<? extends VisitableCommand> getCommitCommand() {
      return CommitCommand.class;
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

         assertTrue(futureReplace.get(5, TimeUnit.SECONDS));

         assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
      }
   }

   @Test
   public void testGetOccursAfterReplaceRunningBeforeWithRemoteException() throws ExecutionException, InterruptedException, BrokenBarrierException, TimeoutException {
      final Cache<Object, String> nonOwnerCache = getFirstNonOwner(key);
      final Cache<Object, String> ownerCache = getFirstOwner(key);

      ownerCache.put(key, firstValue);

      CyclicBarrier barrier = new CyclicBarrier(2);
      addBlockingInterceptorBeforeTx(nonOwnerCache, barrier, ReplaceCommand.class, false);
      RpcManager realManager = nonOwnerCache.getAdvancedCache().getComponentRegistry().getComponent(RpcManager.class);
      RpcManager mockManager = mock(RpcManager.class, AdditionalAnswers.delegatesTo(realManager));

      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            throw new RemoteException("FAIL", new TimeoutException());
         }
         // Only throw exception on the first call just in case if test calls it more than once to fail properly
      }).doAnswer(AdditionalAnswers.delegatesTo(realManager)).when(mockManager).invokeRemotely(anyCollection(), any(ReplicableCommand.class), any(RpcOptions.class));

      TestingUtil.replaceComponent(nonOwnerCache, RpcManager.class, mockManager, true);
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
         try {
            futureReplace.get(5, TimeUnit.SECONDS);
            fail("Test should have thrown an execution exception");
         } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RemoteException);
         }


         try {
            assertEquals(firstValue, futureGet.get(5, TimeUnit.SECONDS));
         } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RemoteException);
         }
      } finally {
         removeAllBlockingInterceptorsFromCache(nonOwnerCache);
         TestingUtil.replaceComponent(nonOwnerCache, RpcManager.class, realManager, true);
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
         Future<String> future = ownerCache.putAsync(key, secondValue);

         // Wait until owner has tried to replicate to backup owner
         backupPutBarrier.await(10, TimeUnit.SECONDS);

         assertEquals(firstValue, ownerCache.getAdvancedCache().getDataContainer().get(key).getValue());
         assertEquals(firstValue, backupOwnerCache.getAdvancedCache().getDataContainer().get(key).getValue());

         // Now remove the interceptor, just so we can add another.  This is okay since it still retains the next
         // interceptor reference properly
         removeAllBlockingInterceptorsFromCache(ownerCache);

         // Add a barrier to block the get from being retrieved on the primary owner
         CyclicBarrier ownerGetBarrier = new CyclicBarrier(2);
         addBlockingInterceptor(ownerCache, ownerGetBarrier, GetKeyValueCommand.class, getL1InterceptorClass(),
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

         // This is async in the LastChance interceptor
         eventually(new Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               return !isInL1(nonOwnerCache, key);
            }
         });

         assertEquals(secondValue, ownerCache.getAdvancedCache().getDataContainer().get(key).getValue());
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

      // We add controlled rpc manager so we can stop the L1 invalidations being sent by the owner and backup.  This
      // way we can ensure these are synchronous
      RpcManager rm = TestingUtil.extractComponent(ownerCache, RpcManager.class);
      ControlledRpcManager crm = new ControlledRpcManager(rm);
      crm.blockBefore(InvalidateL1Command.class);
      TestingUtil.replaceComponent(ownerCache, RpcManager.class, crm, true);

      // We have to do this on backup owner as well since both invalidate now
      RpcManager rm2 = TestingUtil.extractComponent(backupOwnerCache, RpcManager.class);
      ControlledRpcManager crm2 = new ControlledRpcManager(rm2);
      // Make our node block and not return the get yet
      crm2.blockBefore(InvalidateL1Command.class);
      TestingUtil.replaceComponent(backupOwnerCache, RpcManager.class, crm2, true);

      try {
         Future<String> future = fork(new Callable<String>() {

            @Override
            public String call() throws Exception {
               return ownerCache.put(key, secondValue);
            }
         });

         // wait until they all get there, but keep them blocked
         crm.waitForCommandToBlock(10, TimeUnit.SECONDS);
         crm2.waitForCommandToBlock(10, TimeUnit.SECONDS);

         try {
            future.get(1, TimeUnit.SECONDS);
            fail("This should have timed out since, they cannot invalidate L1");
         } catch (TimeoutException e) {
            // We should get a timeout exception as the L1 invalidation commands are blocked and it should be sync
            // so the invalidations are completed before the write completes
         }

         // Now we should let the L1 invalidations go through
         crm.stopBlocking();
         crm2.stopBlocking();

         assertEquals(firstValue, future.get(10, TimeUnit.SECONDS));

         assertIsNotInL1(nonOwnerCache, key);

         assertEquals(secondValue, nonOwnerCache.get(key));
      } finally {
         removeAllBlockingInterceptorsFromCache(ownerCache);
         removeAllBlockingInterceptorsFromCache(backupOwnerCache);
      }
   }
}
