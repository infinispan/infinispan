package org.infinispan.statetransfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Exceptions.expectCompletionException;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(testName = "statetransfer.StateTransferLockTest", groups = "functional")
public class StateTransferLockTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      createCluster(builder, 2);
      waitForClusterToForm();
   }

   @Test
   public void testRewirePreservesTopologyFuture() throws Exception {
      ComponentRegistry cr = ComponentRegistry.of(cache(0));
      StateTransferLock stl = cr.getStateTransferLock();

      int futureTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId() + 100;

      CompletionStage<Void> pendingFuture = stl.topologyFuture(futureTopo);
      assertThat(pendingFuture.toCompletableFuture()).isNotDone();

      cr.rewire();

      stl.notifyTopologyInstalled(futureTopo);

      pendingFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertThat(pendingFuture.toCompletableFuture().isDone()).isTrue();
   }

   @Test
   public void testRewirePreservesTransactionDataFuture() throws Exception {
      ComponentRegistry cr = ComponentRegistry.of(cache(0));
      StateTransferLock stl = cr.getStateTransferLock();

      int futureTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId() + 100;

      stl.notifyTopologyInstalled(futureTopo);
      CompletionStage<Void> pendingFuture = stl.transactionDataFuture(futureTopo + 1);
      assertThat(pendingFuture.toCompletableFuture()).isNotDone();

      cr.rewire();

      stl.notifyTransactionDataReceived(futureTopo + 1);

      pendingFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertThat(pendingFuture.toCompletableFuture().isDone()).isTrue();
   }

   @Test
   public void testLowerTopologyIdIgnored() {
      StateTransferLock stl = ComponentRegistry.of(cache(0)).getStateTransferLock();

      int currentTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId();

      stl.notifyTopologyInstalled(currentTopo + 2);

      CompletionStage<Void> waiting = stl.topologyFuture(currentTopo + 4);
      assertThat(waiting.toCompletableFuture()).isNotDone();

      stl.notifyTopologyInstalled(currentTopo + 1);
      assertThat(waiting.toCompletableFuture()).isNotDone();

      stl.notifyTopologyInstalled(currentTopo + 4);
      assertThat(waiting.toCompletableFuture()).isDone();
   }

   @Test
   public void testLowerTransactionDataTopologyIdIgnored() {
      StateTransferLock stl = ComponentRegistry.of(cache(0)).getStateTransferLock();

      int currentTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId();

      stl.notifyTopologyInstalled(currentTopo + 4);
      stl.notifyTransactionDataReceived(currentTopo + 2);

      CompletionStage<Void> waiting = stl.transactionDataFuture(currentTopo + 4);
      assertThat(waiting.toCompletableFuture()).isNotDone();

      stl.notifyTransactionDataReceived(currentTopo + 1);
      assertThat(waiting.toCompletableFuture()).isNotDone();

      stl.notifyTransactionDataReceived(currentTopo + 4);
      assertThat(waiting.toCompletableFuture()).isDone();
   }

   @Test
   public void testTopologyFutureAfterStop() {
      StateTransferLock stl = ComponentRegistry.of(cache(0)).getStateTransferLock();

      int currentTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId();

      stl.notifyTopologyInstalled(Integer.MAX_VALUE);

      CompletionStage<Void> stage = stl.topologyFuture(currentTopo + 1);
      assertThat(stage.toCompletableFuture()).isCompletedExceptionally();
      expectCompletionException(IllegalLifecycleStateException.class, stage);
   }

   @Test
   public void testTransactionDataFutureAfterStop() {
      StateTransferLock stl = ComponentRegistry.of(cache(0)).getStateTransferLock();

      int currentTopo = cache(0).getAdvancedCache()
            .getDistributionManager().getCacheTopology().getTopologyId();

      stl.notifyTopologyInstalled(Integer.MAX_VALUE);

      CompletionStage<Void> stage = stl.transactionDataFuture(currentTopo + 1);
      assertThat(stage.toCompletableFuture()).isCompletedExceptionally();
      expectCompletionException(IllegalLifecycleStateException.class, stage);
   }
}
