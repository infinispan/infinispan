package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.A;
import static org.infinispan.client.hotrod.impl.PrimaryNearCacheTestFixture.B;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;

import org.testng.annotations.Test;

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
