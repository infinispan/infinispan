package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.A;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.B;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.C;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.channel;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.hash;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.info;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.result;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation.GetWithMetadataResult;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

/**
 * Tests the lifecycle of the primary owner listeners of {@link InvalidatedNearRemoteCache}: the downgrade to the
 * cluster listener, the setup against topologies that change while it runs, and the registration and removal of
 * listeners as owners are lost and regained. The tests drive the production entry points, holding the asynchronous
 * stages pending to control the interleavings.
 */
@Test(groups = "unit", testName = "client.hotrod.impl.PrimaryNearCacheLifecycleTest")
@SuppressWarnings("unchecked")
public class PrimaryNearCacheLifecycleTest extends AbstractInfinispanTest {
   public void testDowngradeSuspendsNearCacheUntilClusterListenerInstalled() throws Exception {
      downgrade(false, false);
   }

   public void testDowngradeSuspendsNearCacheUntilClusterListenerInstalledWithBloomFilter() throws Exception {
      downgrade(true, false);
   }

   public void testFailedDowngradeKeepsNearCacheDisabled() throws Exception {
      downgrade(false, true);
   }

   public void testFailedDowngradeKeepsNearCacheDisabledWithBloomFilter() throws Exception {
      downgrade(true, true);
   }

   public void testListenerRemovedDuringDowngradeUnregistersLateReplacement() throws Exception {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A);
      Object listener = f.listener;
      ClientListenerOperation clusterOp = mock(ClientListenerOperation.class);
      when(f.factory.newAddNearCacheListenerOperation(listener, f.near.getBloomFilterBits())).thenReturn(clusterOp);
      when(f.dispatcher.executeAddListener(clusterOp)).thenAnswer(invocation -> {
         // Removal cannot unregister the replacement until its registration completes.
         f.cache.removeClientListener(listener);
         clearInvocations(f.remote);
         return CompletableFuture.completedFuture(channel());
      });

      f.downgrade();

      // The completed replacement must be removed remotely, even though the user already removed the listener.
      verify(f.remote).removeClientListener(listener);
   }

   private void downgrade(boolean bloom, boolean fail) throws Exception {
      var f = new PrimaryNearCacheTestFixture(bloom);
      f.start(A);
      Object listener = f.listener;
      f.store(1, "cached");

      CompletableFuture<Channel> replacement = new CompletableFuture<>();
      CountDownLatch installing = new CountDownLatch(1);
      ClientListenerOperation clusterOp = mock(ClientListenerOperation.class);
      when(f.factory.newAddNearCacheListenerOperation(listener, f.near.getBloomFilterBits())).thenReturn(clusterOp);
      when(f.dispatcher.executeAddListener(clusterOp)).thenAnswer(invocation -> {
         installing.countDown();
         return replacement;
      });

      Future<Void> downgrade = fork(() -> {
         f.downgrade();
         return null;
      });
      assertThat(installing.await(10, TimeUnit.SECONDS)).isTrue();

      // Nothing invalidates the near cache until the replacement listener is installed
      assertThat(f.near.size()).as("The near cache is cleared by the downgrade").isZero();
      // An entry that lands in the near cache during the gap is not served
      f.store(2, "stale");
      when(f.remote.getWithMetadataAsync(2, null)).thenReturn(CompletableFuture.completedFuture(result("fresh")));
      assertThat(f.cache.getWithMetadataAsync(2).join().getValue()).isEqualTo("fresh");
      verify(f.remote).getWithMetadataAsync(2, null);
      // A read that completes in the gap is not stored
      when(f.remote.getWithMetadataAsync(3, null)).thenReturn(CompletableFuture.completedFuture(result("v3")));
      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("v3");
      assertThat(f.near.get(3)).isNull();
      // A read that starts in the gap, while a write may happen, and completes after the registration is not stored
      CompletableFuture<GetWithMetadataResult<String>> spanning = new CompletableFuture<>();
      when(f.remote.getWithMetadataAsync(4, null)).thenReturn(spanning);
      var spanningRead = f.cache.getWithMetadataAsync(4);

      Channel channel = channel();
      if (fail) {
         replacement.completeExceptionally(new IOException("No cluster listener"));
      } else {
         replacement.complete(channel);
      }
      downgrade.get(10, TimeUnit.SECONDS);
      spanning.complete(result("stale-v4"));
      assertThat(spanningRead.join().getValue()).isEqualTo("stale-v4");
      assertThat(f.near.get(4)).as("A read spanning the downgrade must not be stored").isNull();
      if (!fail) {
         assertThat(f.near.size()).as("Entries stored during the gap are dropped").isZero();
      }

      Channel readChannel = bloom && !fail ? channel : null;
      when(f.remote.getWithMetadataAsync(5, readChannel)).thenReturn(CompletableFuture.completedFuture(result("v5")));
      assertThat(f.cache.getWithMetadataAsync(5).join().getValue()).isEqualTo("v5");
      if (fail) {
         assertThat(f.near.get(5)).as("A failed downgrade leaves the near cache disabled").isNull();
      } else {
         assertThat(f.near.get(5)).as("Caching resumes once the cluster listener is installed").isNotNull();
      }
   }

   public void testTopologyChangesWhileRegisteringAreReconciledOnActivation() throws Exception {
      var f = new PrimaryNearCacheTestFixture(false);
      CacheInfo start = info(hash(A, A));
      f.topology(start);
      Channel channelA = channel();
      Channel channelC = channel();
      CompletableFuture<Channel> addA = new CompletableFuture<>();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A))).thenReturn(addA);
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(C)))
            .thenReturn(CompletableFuture.completedFuture(channelC));

      Future<Channel> starting = fork(() -> f.near.start(f.cache));
      verify(f.dispatcher, timeout(10_000)).executeAddListener(any(ClientListenerOperation.class), eq(A));
      f.store(0, "unchanged");
      f.store(1, "old-owner");

      // Two topologies are accepted while A registers. The cache is not in primary mode yet, so the listeners that
      // run for them do nothing, and only the activation can act on what they accepted.
      f.accept(info(hash(A, B)));
      f.accept(info(hash(A, C)));
      assertThat(f.cache.isPrimaryMode()).isFalse();
      verify(f.dispatcher, never()).executeAddListener(any(ClientListenerOperation.class), eq(C));

      addA.complete(channelA);
      assertThat(starting.get(10, TimeUnit.SECONDS)).isSameAs(channelA);

      assertThat(f.cache.isPrimaryMode()).isTrue();
      // Only the latest topology is acted upon: C is registered, B never was
      verify(f.dispatcher).executeAddListener(any(ClientListenerOperation.class), eq(C));
      verify(f.dispatcher, never()).executeAddListener(any(ClientListenerOperation.class), eq(B));
      assertThat(f.near.get(0).getValue()).as("The unchanged segment stays cached").isEqualTo("unchanged");
      assertThat(f.near.get(1)).as("The segment that changed owner during registration is invalidated").isNull();
      f.assertCacheable(2, true);
      f.assertCacheable(3, true);
   }

   public void testSetupReturnsChannelStillActiveAfterAllRegistrations() {
      var f = new PrimaryNearCacheTestFixture(false);
      Channel disconnected = channel();
      Channel active = channel();
      AtomicInteger registrations = new AtomicInteger();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), any())).thenAnswer(invocation -> {
         if (registrations.getAndIncrement() == 0) {
            return CompletableFuture.completedFuture(disconnected);
         }
         // The first successful listener closes while another owner is still registering.
         when(disconnected.isActive()).thenReturn(false);
         return CompletableFuture.completedFuture(active);
      });

      assertThat(f.start(A, B)).isSameAs(active);
      assertThat(f.cache.isPrimaryMode()).isTrue();
   }

   public void testPartialSetupKeepsFailedOwnerUncachedAcrossTopologies() throws Exception {
      var f = new PrimaryNearCacheTestFixture(false);
      SegmentConsistentHash ch = hash(A, B, A, A);
      CacheInfo info = info(ch);
      f.topology(info);
      Channel channelA = channel();
      Channel channelB = channel();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A)))
            .thenReturn(CompletableFuture.completedFuture(channelA));
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(B)))
            .thenReturn(CompletableFuture.failedFuture(new IOException("B is down")))
            .thenReturn(CompletableFuture.completedFuture(channelB));

      f.near.start(f.cache);

      // One owner failed to register: the cache stays in primary mode, it is not downgraded
      assertThat(f.cache.isPrimaryMode()).isTrue();
      verify(f.factory, never()).newAddNearCacheListenerOperation(any(), anyInt());

      when(f.remote.getWithMetadataAsync(0, channelA)).thenReturn(CompletableFuture.completedFuture(result("a")));
      when(f.remote.getWithMetadataAsync(1, null)).thenReturn(CompletableFuture.completedFuture(result("b")));
      assertThat(f.cache.getWithMetadataAsync(0).join().getValue()).isEqualTo("a");
      assertThat(f.cache.getWithMetadataAsync(1).join().getValue()).isEqualTo("b");
      assertThat(f.near.get(0)).as("The healthy owner is cached").isNotNull();
      assertThat(f.near.get(1)).as("The owner without a listener is not cached").isNull();
      assertThat(f.near.size()).isEqualTo(1);
      // Repeated topologies, including an ownership change elsewhere, do not retry the failed owner.
      f.accept(info(ch));
      f.accept(info(hash(A, B, C, A)));
      verify(f.dispatcher, times(1)).executeAddListener(any(ClientListenerOperation.class), eq(B));
      verify(f.dispatcher, times(1)).executeAddListener(any(ClientListenerOperation.class), eq(A));
      assertThat(f.cache.getWithMetadataAsync(1).join().getValue()).isEqualTo("b");
      assertThat(f.near.get(1)).as("The failed owner remains uncached").isNull();
      assertThat(f.near.get(0)).as("The healthy owner stays cached").isNotNull();
   }

   public void testTopologyDuringFailedRegistrationDoesNotRetry() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(B);
      CacheInfo topology = info(hash(A, B));
      CompletableFuture<Channel> pending = new CompletableFuture<>();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A))).thenReturn(pending);

      f.accept(topology);
      f.accept(topology);
      pending.completeExceptionally(new IOException("A is down"));
      f.accept(topology);

      verify(f.dispatcher, times(1)).executeAddListener(any(ClientListenerOperation.class), eq(A));
      f.assertCacheable(0, false);
      f.assertCacheable(1, true);
   }

   public void testRegainedOwnerRegistersOnlyAfterPendingCleanupCompletes() {
      var f = new PrimaryNearCacheTestFixture(false);
      CacheInfo withA = info(hash(A));
      CacheInfo withoutA = info(hash(B));
      f.start(B);

      List<String> events = Collections.synchronizedList(new ArrayList<>());
      Channel first = channel();
      Channel second = channel();
      AtomicInteger adds = new AtomicInteger();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A))).thenAnswer(invocation -> {
         events.add("add");
         return CompletableFuture.completedFuture(adds.getAndIncrement() == 0 ? first : second);
      });
      CompletableFuture<Channel> firstReady = new CompletableFuture<>();
      when(f.dispatcher.primaryTopologyReady("", first)).thenReturn(firstReady);
      CompletableFuture<Void> cleanup = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.removeOp, A)).thenAnswer(invocation -> {
         events.add("remove");
         return cleanup;
      });

      f.accept(withA);
      // The owner is lost while the registration waits for the topology, and then the registration completes
      f.accept(withoutA);
      firstReady.complete(first);
      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);

      // The owner is regained while the cleanup is pending: its registration waits for the cleanup
      f.accept(withA);
      assertThat(events).containsExactly("add", "remove");
      f.assertCacheable(0, false);

      cleanup.complete(null);
      assertThat(events).containsExactly("add", "remove", "add");
      // Reads use the replacement after cleanup of the old attempt completes.
      f.assertCacheable(1, true);
      verify(f.remote).getWithMetadataAsync(1, second);
      verify(f.dispatcher, times(1)).executeOnSingleAddress(f.removeOp, A);
   }

   public void testDetachedReservationDoesNotRetryRegainedOwner() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A);
      when(f.dispatcher.executeOnSingleAddress(f.removeOp, A)).thenAnswer(invocation -> {
         // B is reserved but has not started registering when cleanup of A starts.
         // Lose and regain B before the original reconciliation resumes.
         f.accept(info(hash(C)));
         f.accept(info(hash(B)));
         f.cache.handlePrimaryListenerDisconnected(B, f.channels.get(B));
         return CompletableFuture.completedFuture(null);
      });

      f.accept(info(hash(B)));

      // The old reservation must not pick up B's new state and retry its lost listener.
      verify(f.dispatcher, times(1)).executeAddListener(any(ClientListenerOperation.class), eq(B));
      f.assertCacheable(0, false);
   }

   public void testOwnerLostAfterActivationIsUnregistered() {
      var f = new PrimaryNearCacheTestFixture(false);
      Channel channelA = channel();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A)))
            .thenReturn(CompletableFuture.completedFuture(channelA));

      assertThat(f.start(A)).isSameAs(channelA);
      f.assertCacheable(1, true);

      f.accept(info(hash(B)));

      // The lost owner is unregistered, and its replacement can cache reads.
      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      f.assertCacheable(2, true);
   }

   public void testOwnerLostBeforeActivationIsUnregistered() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(B);
      Channel channelA = channel();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A)))
            .thenReturn(CompletableFuture.completedFuture(channelA));
      CompletableFuture<Channel> ready = new CompletableFuture<>();
      when(f.dispatcher.primaryTopologyReady("", channelA)).thenReturn(ready);

      f.accept(info(hash(A)));
      f.accept(info(hash(B)));
      ready.complete(channelA);

      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      f.assertCacheable(0, true);
   }
}
