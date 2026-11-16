package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.A;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.B;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.channel;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.hash;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.info;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

/** Tests that Bloom updates fence reads and restore caching only for owners with usable listeners. */
@Test(groups = "unit", testName = "client.hotrod.impl.ServerListenerStateTest")
public class ServerListenerStateTest {
   private static class Fixture extends PrimaryNearCacheTestFixture {
      Fixture() {
         super(true);
         start(A, A, B, B);
         clearInvocations(dispatcher, executor, near);
      }
   }

   public void testReplacementWaitsForBloomCalculation() {
      replacementDuringCalculation(false);
   }

   public void testCalculationFailureDoesNotBlockReplacementListener() {
      replacementDuringCalculation(true);
   }

   private void replacementDuringCalculation(boolean fail) {
      Fixture f = new Fixture();
      AtomicReference<Runnable> calculation = new AtomicReference<>();
      doAnswer(invocation -> {
         calculation.set(invocation.getArgument(0));
         return null;
      }).when(f.executor).execute(any(Runnable.class));
      var update = f.cache.updateBloomFilter().toCompletableFuture();
      doAnswer(invocation -> {
         invocation.<Runnable>getArgument(0).run();
         return null;
      }).when(f.executor).execute(any(Runnable.class));

      f.accept(info(hash(B, B, B, B)));
      f.accept(info(hash(A, A, B, B)));
      verify(f.dispatcher, never()).executeOnSingleAddress(any(), any());
      verify(f.dispatcher, never()).executeAddListener(any(ClientListenerOperation.class), eq(A));
      verify(f.near, never()).calculateBloomBits();
      assertThat(update).isNotDone();
      f.assertCacheable(0, false);

      if (fail) {
         doThrow(new IllegalStateException("Calculation failed")).doCallRealMethod().when(f.near).calculateBloomBits();
      }
      calculation.get().run();
      if (fail) {
         assertThatThrownBy(update::join).hasRootCauseMessage("Calculation failed");
         verify(f.dispatcher, never()).executeOnSingleAddress(f.updateOp, B);
      } else {
         update.join();
         verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      }
      // The detached claim is skipped; only registration initializes the replacement's filter.
      var order = inOrder(f.dispatcher);
      order.verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      order.verify(f.dispatcher).executeAddListener(any(ClientListenerOperation.class), eq(A));
      order.verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.near, times(2)).calculateBloomBits();
      f.assertCacheable(4, true);
      f.assertCacheable(2, true);
   }

   public void testUpdateWaitsForReplacementListener() {
      Fixture f = new Fixture();
      CompletableFuture<Channel> pendingReg = new CompletableFuture<>();
      when(f.dispatcher.executeAddListener(any(ClientListenerOperation.class), eq(A))).thenReturn(pendingReg);

      AtomicReference<Runnable> calculation = new AtomicReference<>();
      doAnswer(invocation -> {
         calculation.set(invocation.getArgument(0));
         return null;
      }).when(f.executor).execute(any(Runnable.class));
      var update = f.cache.updateBloomFilter().toCompletableFuture();
      doAnswer(invocation -> {
         invocation.<Runnable>getArgument(0).run();
         return null;
      }).when(f.executor).execute(any(Runnable.class));

      f.accept(info(hash(B, B, B, B)));
      f.accept(info(hash(A, A, B, B)));

      calculation.get().run();

      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      assertThat(update).isNotDone();
      f.assertCacheable(0, false);
      f.assertCacheable(2, true);

      pendingReg.complete(channel());
      update.join();

      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      f.assertCacheable(0, true);
   }

   public void testReplacementWaitsForInFlightBloomSend() {
      Fixture f = new Fixture();
      CompletableFuture<Void> pending = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A)).thenReturn(pending);
      var update = f.cache.updateBloomFilter().toCompletableFuture();
      f.accept(info(hash(B, B, B, B)));
      f.accept(info(hash(A, A, B, B)));

      verify(f.dispatcher, never()).executeOnSingleAddress(f.removeOp, A);
      verify(f.dispatcher, never()).executeAddListener(any(ClientListenerOperation.class), eq(A));
      f.assertCacheable(0, false);

      pending.complete(null);
      update.join();
      var order = inOrder(f.dispatcher);
      order.verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      order.verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      order.verify(f.dispatcher).executeAddListener(any(ClientListenerOperation.class), eq(A));
      f.assertCacheable(4, true);
   }

   public void testFailedOwnerCleanupReservedBeforeInvalidation() {
      Fixture f = new Fixture();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Send failed")))
            .thenReturn(CompletableFuture.completedFuture(null));
      doAnswer(invocation -> {
         // The failure disabled A, but invalidation has not finished when it loses and regains its segments.
         f.accept(info(hash(B, B, B, B)));
         f.accept(info(hash(A, A, B, B)));
         return invocation.callRealMethod();
      }).when(f.near).invalidateSegments(any(IntSet.class));

      var update = f.cache.updateBloomFilter().toCompletableFuture();
      assertThatThrownBy(update::join).hasRootCauseMessage("Send failed");

      var order = inOrder(f.dispatcher);
      order.verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      order.verify(f.dispatcher).executeAddListener(any(ClientListenerOperation.class), eq(A));
      verify(f.dispatcher, times(1)).executeOnSingleAddress(f.removeOp, A);
      f.assertCacheable(0, true);
      f.assertCacheable(2, true);
   }

   public void testFailedOwnerEntriesInvalidatedBeforeListenerRemoval() {
      Fixture f = new Fixture();
      f.store(0, "stale");
      f.store(2, "healthy");
      when(f.remote.getWithMetadataAsync(0, null))
            .thenReturn(CompletableFuture.completedFuture(PrimaryNearCacheTestFixture.result("fresh")));
      AtomicReference<String> valueDuringRemoval = new AtomicReference<>();
      when(f.dispatcher.executeOnSingleAddress(f.removeOp, A)).thenAnswer(invocation -> {
         valueDuringRemoval.set(f.cache.getWithMetadataAsync(0).join().getValue());
         return CompletableFuture.completedFuture(null);
      });

      // Start cleanup with the server queue idle so opening its gate runs listener removal synchronously.
      Map<?, ?> states = TestingUtil.extractField(f.cache, "serverListenerStates");
      Object state = states.get(A);
      ReflectionUtil.invokeAccessibly(f.cache,
            ReflectionUtil.findMethod(InvalidatedNearRemoteCache.class, "disablePrimaryOwner", SocketAddress.class,
                  state.getClass()), A, state);

      verify(f.dispatcher).executeOnSingleAddress(f.removeOp, A);
      assertThat(valueDuringRemoval.get()).isEqualTo("fresh");
      verify(f.remote).getWithMetadataAsync(0, null);
      assertThat(f.near.get(0)).isNull();
      assertThat(f.near.get(2).getValue()).isEqualTo("healthy");
   }

   public void testConcurrentUpdatesSkipBusyOwners() {
      Fixture f = new Fixture();
      CompletableFuture<Void> pendingA = new CompletableFuture<>();
      CompletableFuture<Void> pendingB = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A)).thenReturn(pendingA);
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B)).thenReturn(pendingB);

      var first = f.cache.updateBloomFilter().toCompletableFuture();
      assertThat(f.cache.updateBloomFilter().toCompletableFuture()).isCompleted();
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      f.assertCacheable(0, false);
      f.assertCacheable(2, false);

      pendingA.complete(null);
      pendingB.complete(null);
      first.join();
      f.assertCacheable(4, true);
      f.assertCacheable(6, true);
   }

   public void testUpdateCompletionRestoresCachingBeforeCallbacks() throws Exception {
      Fixture f = new Fixture();
      CompletableFuture<Void> pendingA = new CompletableFuture<>();
      CompletableFuture<Void> pendingB = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A)).thenReturn(pendingA);
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B)).thenReturn(pendingB);

      var completed = f.cache.updateBloomFilter().thenRun(() -> {
         f.assertCacheable(0, true);
         f.assertCacheable(2, true);
      }).toCompletableFuture();

      pendingA.complete(null);
      pendingB.complete(null);
      completed.get(10, TimeUnit.SECONDS);
   }

   public void testUpdateCompletingAfterDisconnectDoesNotEnableCaching() {
      Fixture f = new Fixture();
      CompletableFuture<Void> pending = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A)).thenReturn(pending);
      var update = f.cache.updateBloomFilter().toCompletableFuture();

      f.cache.handlePrimaryListenerDisconnected(A, f.channels.get(A));
      pending.complete(null);
      update.join();

      f.assertCacheable(0, false);
      f.assertCacheable(2, true);
      verify(f.notifier, never()).failoverClientListener(any(), any());
   }

   public void testExecutorRejectionRestoresCaching() {
      Fixture f = new Fixture();
      doThrow(new RejectedExecutionException("Simulated rejection")).when(f.executor).execute(any());

      var update = f.cache.updateBloomFilter().toCompletableFuture();

      verify(f.executor).execute(any());
      verify(f.dispatcher, never()).executeOnSingleAddress(any(), any());
      assertThatThrownBy(update::join).hasRootCauseInstanceOf(RejectedExecutionException.class);
      f.assertCacheable(0, true);
      f.assertCacheable(2, true);
      doAnswer(invocation -> {
         invocation.<Runnable>getArgument(0).run();
         return null;
      }).when(f.executor).execute(any(Runnable.class));
      f.cache.updateBloomFilter().toCompletableFuture().join();
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
   }

   public void testCalculationFailureRestoresCaching() {
      Fixture f = new Fixture();
      doThrow(new IllegalStateException("Simulated calculation failure")).when(f.near).calculateBloomBits();

      var update = f.cache.updateBloomFilter().toCompletableFuture();

      verify(f.near).calculateBloomBits();
      verify(f.dispatcher, never()).executeOnSingleAddress(any(), any());
      assertThatThrownBy(update::join).isInstanceOf(CompletionException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);
      f.assertCacheable(0, true);
      f.assertCacheable(2, true);
      doCallRealMethod().when(f.near).calculateBloomBits();
      f.cache.updateBloomFilter().toCompletableFuture().join();
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
   }

   public void testPartialSendFailureDisablesOnlyTheFailedOwner() {
      Fixture f = new Fixture();
      f.assertCacheable(0, true);
      f.assertCacheable(2, true);
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Simulated send failure")));

      var update = f.cache.updateBloomFilter().toCompletableFuture();

      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      verify(f.notifier, never()).failoverClientListener(any(), any());
      assertThatThrownBy(update::join).hasRootCauseMessage("Simulated send failure");
      assertThat(f.near.get(0)).as("The healthy owner's cached entries survive").isNotNull();
      assertThat(f.near.get(2)).as("The failed owner's cached entries are invalidated").isNull();
      f.assertCacheable(4, true);
      f.assertCacheable(6, false);
   }

   public void testAllSendsFailing() {
      Fixture f = new Fixture();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Failure A")));
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Failure B")));

      var update = f.cache.updateBloomFilter().toCompletableFuture();

      assertThatThrownBy(update::join).isInstanceOf(CompletionException.class);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);
      f.assertCacheable(0, false);
      f.assertCacheable(2, false);
      assertThat(f.cache.hasActivePrimaryListeners()).isFalse();
   }

   public void testFailedUpdateDoesNotResumeCachingDuringANewerUpdate() {
      Fixture f = new Fixture();
      CompletableFuture<Void> pendingB = new CompletableFuture<>();
      CompletableFuture<Void> pendingY = new CompletableFuture<>();
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, A))
            .thenReturn(CompletableFuture.completedFuture(null)).thenReturn(pendingY);
      when(f.dispatcher.executeOnSingleAddress(f.updateOp, B)).thenReturn(pendingB);

      var x = f.cache.updateBloomFilter().toCompletableFuture();
      var y = f.cache.updateBloomFilter().toCompletableFuture();
      verify(f.dispatcher, times(2)).executeOnSingleAddress(f.updateOp, A);
      verify(f.dispatcher).executeOnSingleAddress(f.updateOp, B);

      pendingB.completeExceptionally(new RuntimeException("Simulated send failure"));
      assertThatThrownBy(x::join).hasRootCauseMessage("Simulated send failure");
      assertThat(y).isNotDone();
      f.assertCacheable(0, false);
      f.assertCacheable(2, false);

      pendingY.complete(null);
      y.join();
      f.assertCacheable(4, true);
   }
}
