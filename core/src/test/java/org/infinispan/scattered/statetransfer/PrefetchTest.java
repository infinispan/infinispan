package org.infinispan.scattered.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scattered.statetransfer.PrefetchTest")
@CleanupAfterMethod
public class PrefetchTest extends MultipleCacheManagersTest {
   ControlledConsistentHashFactory chf;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      chf = new ControlledConsistentHashFactory.Scattered(0);
      cb.clustering().cacheMode(CacheMode.SCATTERED_SYNC).hash().numSegments(1).consistentHashFactory(chf);

      createCluster(cb, 3);
      waitForClusterToForm();
   }

   public void testPrefetch00() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      // Invoke prefetch before getting any data, receive before getting any data
      testPrefetch(0, 0);
   }

   public void testPrefetch01() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      // Invoke prefetch before getting any data, receive after getting metadata
      testPrefetch(0, 1);
   }

   public void testPrefetch02() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      // Invoke prefetch before getting any data, receive before getting full data
      testPrefetch(0, 2);
   }

   public void testPrefetch11() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      // Invoke prefetch when having metadata, receive before getting full data
      testPrefetch(1, 1);
   }

   public void testPrefetch12() throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      // Invoke prefetch when having metadata, receive after getting full data
      testPrefetch(1, 2);
   }

   private void testPrefetch(int invokePhase, int receivePhase) throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      cache(1).put("key", "v0");

      CyclicBarrier beforeBarrier = new CyclicBarrier(2);
      CyclicBarrier afterBarrier = new CyclicBarrier(2);
      BlockingBefore beforeInterceptor = new BlockingBefore(beforeBarrier);
      BlockingAfter afterInterceptor = new BlockingAfter(afterBarrier);
      // let's pick STI as the place since this is sufficiently high in the stack
      cache(2).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorBefore(beforeInterceptor, StateTransferInterceptor.class);
      cache(2).getAdvancedCache().getAsyncInterceptorChain()
            .addInterceptorBefore(afterInterceptor, StateTransferInterceptor.class);

      // cache(2) will become owner after we stop cache(1)
      chf.setOwnerIndexes(1);
      cache(1).stop();

      beforeBarrier.await(10, TimeUnit.SECONDS);

      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache(2));
      // ignore ClusteredGetAllCommands that are used to fetch values for value transfer
      // ignore InvalidateVersionCommand invalidating
      // ignore PutKeyValueCommand replicating the write to other nodes
      // ignore PutMapCommand that backs up the entry after reconciliation
      controlledRpcManager.excludeCommands(ClusteredGetAllCommand.class, InvalidateVersionsCommand.class, PutKeyValueCommand.class, PutMapCommand.class);

      if (invokePhase > 0) {
         // unblock key-transfer
         beforeBarrier.await(10, TimeUnit.SECONDS);
         // make sure that the key transfer is applied
         afterBarrier.await(10, TimeUnit.SECONDS);
         afterBarrier.await(10, TimeUnit.SECONDS);
         // wait for the value-transfer
         beforeBarrier.await(10, TimeUnit.SECONDS);
      }

      // don't block the prefetching put
      beforeInterceptor.suspend(true);

      Future<Object> future = fork(() -> cache(2).put("key", "v1"));
      ControlledRpcManager.BlockedRequest blockedPrefetch = controlledRpcManager.expectCommand(ClusteredGetCommand.class);
      if (invokePhase > 0) {
         // When we have the remote metadata we should target only single owner
         assertEquals(Collections.singleton(address(0)), blockedPrefetch.getTargets());
      }
      ControlledRpcManager.SentRequest remotePrefetch = blockedPrefetch.send();

      if (receivePhase > 0 && invokePhase == 0) {
         // block the value-transfer
         beforeInterceptor.suspend(false);
         // release key transfer
         beforeBarrier.await(10, TimeUnit.SECONDS);
         // make sure that the key transfer is applied
         afterBarrier.await(10, TimeUnit.SECONDS);
         afterBarrier.await(10, TimeUnit.SECONDS);
      }
      if (receivePhase > 1) {
         if (invokePhase == 0) {
            // await value transfer
            beforeBarrier.await(10, TimeUnit.SECONDS);
         }
         // unblock value transfer
         beforeBarrier.await(10, TimeUnit.SECONDS);
         // make sure that the value transfer is applied
         afterBarrier.await(10, TimeUnit.SECONDS);
         afterBarrier.await(10, TimeUnit.SECONDS);
      }

      afterInterceptor.suspend(true);
      remotePrefetch.receiveAll();

      assertEquals("v0", future.get(10, TimeUnit.SECONDS));

      controlledRpcManager.stopBlocking();
      // release any threads waiting
      beforeBarrier.reset();
      afterBarrier.reset();
   }

   private static boolean isStateTransferPut(VisitableCommand command) {
      return command instanceof PutKeyValueCommand &&
            ((PutKeyValueCommand) command).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER);
   }

   private static class BlockingBefore extends BlockingInterceptor<PutKeyValueCommand> {
      public BlockingBefore(CyclicBarrier barrier) {
         super(barrier, false, true, PrefetchTest::isStateTransferPut);
      }
   }

   private static class BlockingAfter extends BlockingInterceptor<PutKeyValueCommand> {
      public BlockingAfter(CyclicBarrier barrier) {
         super(barrier, true, true, PrefetchTest::isStateTransferPut);
      }
   }
}
