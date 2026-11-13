package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.A;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.B;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.C;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.hash;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.info;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.result;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation.GetWithMetadataResult;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(groups = "unit", testName = "client.hotrod.impl.PrimaryNearCacheReadRaceTest")
public class PrimaryNearCacheReadRaceTest extends AbstractInfinispanTest {
   public void testInvalidationBetweenValidationAndReplace() {
      runRace(true);
   }

   public void testInvalidationDuringInFlightRead() {
      runRace(false);
   }

   private void runRace(boolean invalidateOnReplace) {
      var f = new PrimaryNearCacheTestFixture(false);
      var channel = f.start(A, A);
      CompletableFuture<GetWithMetadataResult<String>> response = new CompletableFuture<>();
      when(f.remote.getWithMetadataAsync(0, channel)).thenReturn(response);
      if (invalidateOnReplace) {
         // Pause at insertion, after the response validation has already succeeded.
         doAnswer(invocation -> {
            f.accept(info(hash(B, A)));
            return invocation.callRealMethod();
         }).when(f.near).replace(any(), any(), any());
      }
      var read = f.cache.getWithMetadataAsync(0);
      if (!invalidateOnReplace) {
         // An A -> B -> A transition can invalidate this segment while A stays active for another segment.
         f.accept(info(hash(B, A)));
         f.accept(info(hash(A, A)));
      }
      response.complete(result("old"));
      assertThat(read.join().getValue()).isEqualTo("old");
      assertThat(f.near.get(0)).as("A response spanning a segment invalidation must not remain cached").isNull();
   }

   public void testCachedEntryInvalidatedInPreTopologyUpdate() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A);
      f.store(1, "cached-v1");
      var oldInfo = f.latest;
      var newInfo = info(hash(B));
      f.topologyListener.beforeTopologyUpdate(oldInfo, newInfo);

      assertThat(f.near.get(1)).as("Entry must be evicted before the new owners are published").isNull();

      // An entry filled after the pre-listener must survive the post-listener.
      f.store(1, "fresh-v1");
      f.topology(newInfo);
      f.topologyListener.afterTopologyUpdate(oldInfo, newInfo);
      assertThat(f.near.get(1)).as("The post-listener must preserve a fresh refill").isNotNull();
   }

   public void testReadCrossingModeDowngradeCannotPopulateNearCache() {
      var f = new PrimaryNearCacheTestFixture(false);
      var channel = f.start(A);
      CompletableFuture<GetWithMetadataResult<String>> response = new CompletableFuture<>();
      when(f.remote.getWithMetadataAsync(1, channel)).thenReturn(response);
      var read = f.cache.getWithMetadataAsync(1);

      f.downgrade();
      response.complete(result("stale"));

      assertThat(read.join().getValue()).isEqualTo("stale");
      assertThat(f.near.get(1)).as("Read crossing a mode switch must not be cached").isNull();
   }

   public void testReadWhenConsistentHashIsUnavailableFallsBackToNormalRouting() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A);
      when(f.dispatcher.getConsistentHash("")).thenReturn(null);
      when(f.remote.getWithMetadataAsync(1, null)).thenReturn(CompletableFuture.completedFuture(result("fallback-val")));

      assertThat(f.cache.getWithMetadataAsync(1).join().getValue()).isEqualTo("fallback-val");
      verify(f.remote).getWithMetadataAsync(1, null);
      assertThat(f.near.get(1)).isNull();
      assertThat(f.near.size()).isZero();
   }

   public void testFailedRegistrationStageUnregistersServerListener() {
      var f = new PrimaryNearCacheTestFixture(false);
      var channelA = f.start(A);
      var channelB = PrimaryNearCacheTestFixture.channel();
      f.channels.put(B, channelB);
      when(f.dispatcher.primaryTopologyReady("", channelB))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Simulated topology ready failure")));

      f.accept(info(hash(A, B)));

      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, B);
      when(f.remote.getWithMetadataAsync(0, channelA)).thenReturn(CompletableFuture.completedFuture(result("a")));
      when(f.remote.getWithMetadataAsync(1, null)).thenReturn(CompletableFuture.completedFuture(result("b")));
      assertThat(f.cache.getWithMetadataAsync(0).join().getValue()).isEqualTo("a");
      assertThat(f.cache.getWithMetadataAsync(1).join().getValue()).isEqualTo("b");
      assertThat(f.near.get(0)).as("The existing listener remains usable").isNotNull();
      assertThat(f.near.get(1)).as("The failed registration cannot cache reads").isNull();
   }

   public void testSegmentCountChangePreservesOwnerFencing() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A, B);
      f.accept(info(hash(C, C, C, C)));
      when(f.remote.getWithMetadataAsync(3, f.channels.get(C)))
            .thenReturn(CompletableFuture.completedFuture(result("before")));
      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("before");

      // The last segment moves before its new owner's listener is installed.
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(B)))
            .thenReturn(new CompletableFuture<>());
      f.accept(info(hash(C, C, C, B)));
      when(f.remote.getWithMetadataAsync(3, null)).thenReturn(CompletableFuture.completedFuture(result("after")));

      assertThat(f.cache.getWithMetadataAsync(3).join().getValue())
            .as("A segment without a registered listener must be fetched remotely after the segment count changes")
            .isEqualTo("after");
   }

   public void testSmallerSegmentCountInvalidatesOldEntries() {
      var f = new PrimaryNearCacheTestFixture(false);
      f.start(A, B, A, B);
      f.store(3, "before");
      f.accept(info(hash(A, B)));
      when(f.remote.getWithMetadataAsync(3, f.channels.get(B)))
            .thenReturn(CompletableFuture.completedFuture(result("after")));

      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("after");
      f.assertCacheable(7, true);
   }

   public void testReadCrossingSegmentCountChangeCannotPopulateNearCache() {
      var f = new PrimaryNearCacheTestFixture(false);
      var channel = f.start(A, A);
      CompletableFuture<GetWithMetadataResult<String>> response = new CompletableFuture<>();
      when(f.remote.getWithMetadataAsync(3, channel)).thenReturn(response);
      var read = f.cache.getWithMetadataAsync(3);

      f.accept(info(hash(A, A, A, A)));
      response.complete(result("before"));

      assertThat(read.join().getValue()).isEqualTo("before");
      assertThat(f.near.get(3)).as("A response from the previous segment mapping must not remain cached").isNull();
      when(f.remote.getWithMetadataAsync(3, channel)).thenReturn(CompletableFuture.completedFuture(result("after")));
      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("after");
   }

   public void testSegmentCountReturnsDuringRegistration() throws Exception {
      var f = new PrimaryNearCacheTestFixture(false);
      var original = info(hash(A, A, A, A));
      f.topology(original);
      AtomicBoolean firstSnapshot = new AtomicBoolean(true);
      when(f.dispatcher.getCacheInfo("")).thenAnswer(invocation -> {
         var snapshot = f.latest;
         if (firstSnapshot.getAndSet(false)) {
            // A concurrent update arrives just after the setup snapshot was read.
            f.topology(info(hash(A, A)));
         }
         return snapshot;
      });
      var channel = PrimaryNearCacheTestFixture.channel();
      CompletableFuture<Channel> registration = new CompletableFuture<>();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A))).thenReturn(registration);
      var starting = fork(() -> f.near.start(f.cache));
      verify(f.dispatcher, timeout(10_000)).executeAddListener(any(ClientListenerOperation.class), eq(A));
      f.accept(original);
      registration.complete(channel);
      starting.get(10, TimeUnit.SECONDS);

      when(f.remote.getWithMetadataAsync(3, channel)).thenReturn(CompletableFuture.completedFuture(result("before")));
      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("before");
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(B))).thenReturn(new CompletableFuture<>());
      f.accept(info(hash(A, A, A, B)));
      when(f.remote.getWithMetadataAsync(3, null)).thenReturn(CompletableFuture.completedFuture(result("after")));

      assertThat(f.cache.getWithMetadataAsync(3).join().getValue()).isEqualTo("after");
   }
}
