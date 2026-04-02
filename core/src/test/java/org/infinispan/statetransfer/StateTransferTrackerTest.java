package org.infinispan.statetransfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.StateTransferTrackerTest")
public class StateTransferTrackerTest {

   private static final String CACHE_NAME = "test-cache";

   public void testListenerCompletion() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);

      assertThat(stt.isStateTransferInProgress()).isFalse();

      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();

      int topologyId = 1;

      // There is no topology yet, so it doesn't complete the listener.
      assertThat(cs).isNotDone();

      stt.startStateConsumer(topologyId);

      assertThat(stt.isStateTransferInProgress()).isTrue();

      stt.startStateProvider(topologyId);
      stt.completeStateProvider(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      stt.completeStateConsumer(topologyId);

      // It is not complete because it is still pending the stable topology.
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      stt.cacheTopologyUpdated(createCacheTopology(topologyId, false));

      // Received topology was not stable.
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      stt.cacheTopologyUpdated(createCacheTopology(topologyId, true));

      assertThat(stt.isStateTransferInProgress()).isFalse();
      assertThat(cs).isCompleted();
   }

   public void testListenerCompleteDirectly() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);

      assertThat(stt.isStateTransferInProgress()).isFalse();

      int topologyId = 1;
      CacheTopology ct = createCacheTopology(topologyId, true);
      stt.cacheTopologyUpdated(ct);

      CompletableFuture<Void> cf = stt.onStateTransferCompleted((topology, t) -> {
         assertThat(topology).isSameAs(ct);
         assertThat(t).isNull();
         return true;
      }).toCompletableFuture();

      assertThat(cf).isDone();
      assertThat(stt.isStateTransferInProgress()).isFalse();
   }

   public void testListenerInstalledAfterTasks() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);
      assertThat(stt.isStateTransferInProgress()).isFalse();

      int topologyId = 1;

      stt.startStateConsumer(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();
      stt.completeStateConsumer(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();

      stt.startStateProvider(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();
      stt.completeStateProvider(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();

      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();
      assertThat(cs).isNotDone();

      stt.cacheTopologyUpdated(createCacheTopology(topologyId, true));

      assertThat(cs).isDone();
      assertThat(stt.isStateTransferInProgress()).isFalse();
   }

   public void testSupersededByHigherTopology() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);
      assertThat(stt.isStateTransferInProgress()).isFalse();

      int topologyId = 1;

      stt.startStateConsumer(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();

      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();
      assertThat(cs).isNotDone();

      stt.startStateProvider(topologyId + 1);
      assertThat(cs).isNotDone();
      assertThat(stt.isStateTransferInProgress()).isTrue();

      // Installing the topology will complete everything.
      stt.cacheTopologyUpdated(createCacheTopology(topologyId + 1, true));
   }

   public void testInstallingTopologyOnly() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);
      assertThat(stt.isStateTransferInProgress()).isFalse();

      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();
      assertThat(cs).isNotDone();

      stt.cacheTopologyUpdated(createCacheTopology(1, true));
      assertThat(cs).isDone();
   }

   public void testOnlyProvider() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);
      assertThat(stt.isStateTransferInProgress()).isFalse();

      int topologyId = 42;
      stt.startStateProvider(topologyId);

      assertThat(stt.isStateTransferInProgress()).isTrue();
      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();
      assertThat(cs).isNotDone();

      stt.completeStateProvider(topologyId);
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      stt.cacheTopologyUpdated(createCacheTopology(topologyId, true));
   }

   public void testListenerKeptInstalled() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);
      assertThat(stt.isStateTransferInProgress()).isFalse();

      int expectedTopology = 42;

      stt.cacheTopologyUpdated(createCacheTopology(expectedTopology - 1, true));

      // Even though the listener is invoked directly, the topologyId does not match.
      // Listener should be kept and not complete right away.
      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> ct.getTopologyId() == expectedTopology).toCompletableFuture();
      assertThat(cs).isNotDone();

      // There is no state transfer in place right now, but the listener should still remain installed
      assertThat(stt.isStateTransferInProgress()).isFalse();

      stt.startStateConsumer(expectedTopology);
      stt.completeStateConsumer(expectedTopology);
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      // Non-stable topology does not complete the listener.
      stt.cacheTopologyUpdated(createCacheTopology(expectedTopology, false));
      assertThat(stt.isStateTransferInProgress()).isTrue();
      assertThat(cs).isNotDone();

      // Should complete now.
      stt.cacheTopologyUpdated(createCacheTopology(expectedTopology, true));
      assertThat(stt.isStateTransferInProgress()).isFalse();
      assertThat(cs).isDone();
   }

   public void testStableTopologyDuringInFlightTransfer() {
      StateTransferTracker.CacheStateTransferTracker stt = new StateTransferTracker().forCache(CACHE_NAME);

      stt.startStateConsumer(5);

      CompletableFuture<Void> cs = stt.onStateTransferCompleted((ct, t) -> true).toCompletableFuture();
      assertThat(cs).isNotDone();

      // A stable topology with a higher ID arrives while consumer for topology 5 is still pending.
      // Before the fix, consumer.complete(5, null) would throw CancellationException because
      // the consumer's tracked ID (5) != the stable topology ID, causing the listener to never fire.
      stt.cacheTopologyUpdated(createCacheTopology(7, true));

      assertThat(stt.isStateTransferInProgress()).isFalse();
      assertThat(cs).isCompleted();
   }

   private static CacheTopology createCacheTopology(int id, boolean stable) {
      CacheTopology ct = mock(CacheTopology.class);
      when(ct.getTopologyId()).thenReturn(id);
      when(ct.getPendingCH()).thenReturn(stable ? null : mock(ConsistentHash.class));
      return ct;
   }
}
