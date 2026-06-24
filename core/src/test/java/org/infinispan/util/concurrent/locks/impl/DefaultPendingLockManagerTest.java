package org.infinispan.util.concurrent.locks.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.PendingLockPromise;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "util.concurrent.locks.impl.DefaultPendingLockManagerTest")
public class DefaultPendingLockManagerTest extends AbstractInfinispanTest {

   private static final int CURRENT_TOPOLOGY = 10;
   private static final int OLD_TOPOLOGY = 5;
   private static final long LOCK_TIMEOUT = 10_000;

   private DefaultPendingLockManager manager;
   private ControlledTimeService timeService;
   private List<CacheTransaction> transactions;
   private Queue<Runnable> scheduledTimeouts;

   @BeforeMethod
   public void setUp() {
      manager = new DefaultPendingLockManager();
      timeService = new ControlledTimeService();
      transactions = new ArrayList<>();
      scheduledTimeouts = new ConcurrentLinkedDeque<>();

      TransactionTable transactionTable = mock(TransactionTable.class);
      when(transactionTable.getMinTopologyId()).thenReturn(OLD_TOPOLOGY);
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenAnswer(inv -> transactions);

      LocalizedCacheTopology cacheTopology = mock(LocalizedCacheTopology.class);
      when(cacheTopology.getTopologyId()).thenReturn(CURRENT_TOPOLOGY);
      DistributionManager distributionManager = mock(DistributionManager.class);
      when(distributionManager.getCacheTopology()).thenReturn(cacheTopology);

      TestingUtil.inject(manager,
            transactionTable,
            timeService,
            distributionManager,
            TestingUtil.named(TIMEOUT_SCHEDULE_EXECUTOR, createMockTimeoutExecutor()));
   }

   @AfterMethod
   public void tearDown() {
      transactions.clear();
      scheduledTimeouts.clear();
   }

   public void testNoPendingCheckWhenMinTopologyMatchesCurrent() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      TransactionTable table = mock(TransactionTable.class);
      when(table.getMinTopologyId()).thenReturn(CURRENT_TOPOLOGY);
      when(table.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(table.getRemoteTransactions()).thenAnswer(inv -> transactions);

      LocalizedCacheTopology topology = mock(LocalizedCacheTopology.class);
      when(topology.getTopologyId()).thenReturn(CURRENT_TOPOLOGY);
      DistributionManager dm = mock(DistributionManager.class);
      when(dm.getCacheTopology()).thenReturn(topology);

      DefaultPendingLockManager noCheckManager = new DefaultPendingLockManager();
      TestingUtil.inject(noCheckManager, table, timeService, dm,
            TestingUtil.named(TIMEOUT_SCHEDULE_EXECUTOR, createMockTimeoutExecutor()));

      PendingLockPromise promise = noCheckManager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testNoPendingTransactions() {
      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testNoMatchingTransactionsAtCurrentTopology() {
      GlobalTransaction txGtx = newGlobalTransaction();
      transactions.add(transactionAt(txGtx, CURRENT_TOPOLOGY, "key-1", new CompletableFuture<>()));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testCompletedFutureFilteredFromScan() {
      CompletableFuture<Void> alreadyDone = CompletableFuture.completedFuture(null);
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", alreadyDone));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testOwnTransactionFilteredFromScan() {
      GlobalTransaction requestingGtx = newGlobalTransaction();
      transactions.add(pendingTransaction(requestingGtx, "key-1", new CompletableFuture<>()));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(requestingGtx), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testPendingTransactionCompletesNormally() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isNotSameAs(PendingLockPromise.NO_OP);
      assertThat(promise.isReady()).isFalse();
      assertThat(promise.hasTimedOut()).isFalse();

      releaseFuture.complete(null);
      assertThat(promise.isReady()).isTrue();
      assertThat(promise.hasTimedOut()).isFalse();
   }

   public void testSignalReusedForSameKey() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      GlobalTransaction requester1 = newGlobalTransaction();
      GlobalTransaction requester2 = newGlobalTransaction();

      PendingLockPromise promise1 = manager.checkPendingTransactionsForKey(
            context(requester1), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      PendingLockPromise promise2 = manager.checkPendingTransactionsForKey(
            context(requester2), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      assertThat(promise1.isReady()).isFalse();
      assertThat(promise2.isReady()).isFalse();

      releaseFuture.complete(null);
      assertThat(promise1.isReady()).isTrue();
      assertThat(promise2.isReady()).isTrue();
   }

   public void testSignalReplacedAfterCompletion() {
      CompletableFuture<Void> releaseFuture1 = new CompletableFuture<>();
      GlobalTransaction blockerGtx1 = newGlobalTransaction();
      transactions.add(pendingTransaction(blockerGtx1, "key-1", releaseFuture1));

      PendingLockPromise promise1 = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise1.isReady()).isFalse();

      releaseFuture1.complete(null);
      assertThat(promise1.isReady()).isTrue();

      transactions.clear();
      CompletableFuture<Void> releaseFuture2 = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture2));

      PendingLockPromise promise2 = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise2).isNotSameAs(PendingLockPromise.NO_OP);
      assertThat(promise2.isReady()).isFalse();

      releaseFuture2.complete(null);
      assertThat(promise2.isReady()).isTrue();
   }

   public void testIsOnlyPendingForSameGtxReturnsNoOp() {
      GlobalTransaction blockerGtx = newGlobalTransaction();
      transactions.add(pendingTransaction(blockerGtx, "key-1", new CompletableFuture<>()));

      GlobalTransaction differentRequester = newGlobalTransaction();
      PendingLockPromise firstPromise = manager.checkPendingTransactionsForKey(
            context(differentRequester), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(firstPromise).isNotSameAs(PendingLockPromise.NO_OP);

      PendingLockPromise selfPromise = manager.checkPendingTransactionsForKey(
            context(blockerGtx), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(selfPromise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testIsOnlyPendingForMixedGtxReturnsPendingPromise() {
      GlobalTransaction blocker1 = newGlobalTransaction();
      GlobalTransaction blocker2 = newGlobalTransaction();
      transactions.add(pendingTransaction(blocker1, "key-1", new CompletableFuture<>()));
      transactions.add(pendingTransaction(blocker2, "key-1", new CompletableFuture<>()));

      GlobalTransaction differentRequester = newGlobalTransaction();
      PendingLockPromise firstPromise = manager.checkPendingTransactionsForKey(
            context(differentRequester), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(firstPromise).isNotSameAs(PendingLockPromise.NO_OP);

      PendingLockPromise selfPromise = manager.checkPendingTransactionsForKey(
            context(blocker1), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(selfPromise).isNotSameAs(PendingLockPromise.NO_OP);
   }

   public void testIsOnlyPendingForAfterPartialCompletion() {
      GlobalTransaction blocker1 = newGlobalTransaction();
      GlobalTransaction blocker2 = newGlobalTransaction();
      CompletableFuture<Void> release1 = new CompletableFuture<>();
      CompletableFuture<Void> release2 = new CompletableFuture<>();
      transactions.add(pendingTransaction(blocker1, "key-1", release1));
      transactions.add(pendingTransaction(blocker2, "key-1", release2));

      GlobalTransaction differentRequester = newGlobalTransaction();
      manager.checkPendingTransactionsForKey(
            context(differentRequester), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      release1.complete(null);

      PendingLockPromise selfPromise = manager.checkPendingTransactionsForKey(
            context(blocker2), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(selfPromise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testTimeout() {
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", new CompletableFuture<>()));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise.isReady()).isFalse();

      timeService.advance(LOCK_TIMEOUT + 1);
      fireLastScheduledTimeout();

      assertThat(promise.isReady()).isTrue();
      assertThat(promise.hasTimedOut()).isTrue();
      assertThatThrownBy(() -> promise.toInvocationStage().toCompletableFuture()
            .get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(TimeoutException.class);
   }

   public void testMultipleWaitersHeadTimesOutNextSurvives() {
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", new CompletableFuture<>()));

      PendingLockPromise head = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      timeService.advance(1_000);

      PendingLockPromise tail = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      timeService.advance(LOCK_TIMEOUT - 1);
      fireLastScheduledTimeout();

      assertThat(head.hasTimedOut()).isTrue();
      assertThat(tail.isReady()).isFalse();
      assertThat(tail.hasTimedOut()).isFalse();
   }

   public void testWaiterAfterCompletionIsImmediatelyReady() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      PendingLockPromise first = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      releaseFuture.complete(null);
      assertThat(first.isReady()).isTrue();

      PendingLockPromise late = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(late.isReady()).isTrue();
   }

   public void testMultiKeyComposite() {
      CompletableFuture<Void> release1 = new CompletableFuture<>();
      CompletableFuture<Void> release2 = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", release1));
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-2", release2));

      GlobalTransaction requester = newGlobalTransaction();
      Collection<Object> keys = List.of("key-1", "key-2");
      PendingLockPromise composite = manager.checkPendingTransactionsForKeys(
            context(requester), keys, LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      assertThat(composite).isNotSameAs(PendingLockPromise.NO_OP);
      assertThat(composite.isReady()).isFalse();

      release1.complete(null);
      assertThat(composite.isReady()).isFalse();

      release2.complete(null);
      assertThat(composite.isReady()).isTrue();
      assertThat(composite.hasTimedOut()).isFalse();
   }

   public void testMultiKeyTimeoutPropagation() {
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", new CompletableFuture<>()));
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-2", new CompletableFuture<>()));

      Collection<Object> keys = List.of("key-1", "key-2");
      PendingLockPromise composite = manager.checkPendingTransactionsForKeys(
            context(newGlobalTransaction()), keys, LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      timeService.advance(LOCK_TIMEOUT + 1);
      scheduledTimeouts.forEach(Runnable::run);

      assertThat(composite.isReady()).isTrue();
      assertThat(composite.hasTimedOut()).isTrue();
   }

   public void testMultiKeySingleKeyOptimization() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      PendingLockPromise promise = manager.checkPendingTransactionsForKeys(
            context(newGlobalTransaction()), List.of("key-1"), LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isNotSameAs(PendingLockPromise.NO_OP);

      releaseFuture.complete(null);
      assertThat(promise.isReady()).isTrue();
   }

   public void testMultiKeyMixedPendingAndNoPending() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      Collection<Object> keys = List.of("key-1", "key-no-pending");
      PendingLockPromise promise = manager.checkPendingTransactionsForKeys(
            context(newGlobalTransaction()), keys, LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      assertThat(promise).isNotSameAs(PendingLockPromise.NO_OP);
      assertThat(promise.isReady()).isFalse();

      releaseFuture.complete(null);
      assertThat(promise.isReady()).isTrue();
   }

   public void testMultiKeyAllNoOp() {
      PendingLockPromise promise = manager.checkPendingTransactionsForKeys(
            context(newGlobalTransaction()), List.of("key-1", "key-2"), LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      assertThat(promise).isSameAs(PendingLockPromise.NO_OP);
   }

   public void testListenerNotifiedOnCompletion() {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      PendingLockPromise promise = manager.checkPendingTransactionsForKey(
            context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      CompletableFuture<Void> listenerFired = new CompletableFuture<>();
      promise.addListener(() -> listenerFired.complete(null));
      assertThat(listenerFired).isNotDone();

      releaseFuture.complete(null);
      assertThat(listenerFired).isDone();
   }

   public void testConcurrentCheckOnSameKey() throws Exception {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      int threadCount = 10;
      CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
      List<Future<PendingLockPromise>> futures = new ArrayList<>(threadCount);

      for (int i = 0; i < threadCount; i++) {
         futures.add(fork(() -> {
            barrier.await(10, TimeUnit.SECONDS);
            return manager.checkPendingTransactionsForKey(
                  context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
         }));
      }

      barrier.await(10, TimeUnit.SECONDS);

      List<PendingLockPromise> promises = new ArrayList<>(threadCount);
      for (Future<PendingLockPromise> f : futures) {
         promises.add(f.get(10, TimeUnit.SECONDS));
      }

      for (PendingLockPromise promise : promises) {
         assertThat(promise).isNotSameAs(PendingLockPromise.NO_OP);
         assertThat(promise.isReady()).isFalse();
      }

      releaseFuture.complete(null);

      for (PendingLockPromise promise : promises) {
         assertThat(promise.isReady()).isTrue();
         assertThat(promise.hasTimedOut()).isFalse();
      }
   }

   public void testConcurrentCheckOnSameKeyDuringCompletion() throws Exception {
      CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
      transactions.add(pendingTransaction(newGlobalTransaction(), "key-1", releaseFuture));

      int threadCount = 10;
      CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
      List<Future<PendingLockPromise>> futures = new ArrayList<>(threadCount);

      for (int i = 0; i < threadCount; i++) {
         futures.add(fork(() -> {
            barrier.await(10, TimeUnit.SECONDS);
            return manager.checkPendingTransactionsForKey(
                  context(newGlobalTransaction()), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
         }));
      }

      barrier.await(10, TimeUnit.SECONDS);
      releaseFuture.complete(null);

      for (Future<PendingLockPromise> f : futures) {
         PendingLockPromise promise = f.get(10, TimeUnit.SECONDS);
         if (promise != PendingLockPromise.NO_OP) {
            assertThat(promise.isReady()).isTrue();
            assertThat(promise.hasTimedOut()).isFalse();
         }
      }
   }

   public void testIndependentKeysHaveIndependentSignals() {
      CompletableFuture<Void> release1 = new CompletableFuture<>();
      CompletableFuture<Void> release2 = new CompletableFuture<>();
      GlobalTransaction blockerGtx = newGlobalTransaction();
      transactions.add(pendingTransaction(blockerGtx, "key-1", release1));
      transactions.add(pendingTransaction(blockerGtx, "key-2", release2));

      GlobalTransaction requester = newGlobalTransaction();
      PendingLockPromise p1 = manager.checkPendingTransactionsForKey(
            context(requester), "key-1", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
      PendingLockPromise p2 = manager.checkPendingTransactionsForKey(
            context(requester), "key-2", LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

      release1.complete(null);
      assertThat(p1.isReady()).isTrue();
      assertThat(p2.isReady()).isFalse();

      release2.complete(null);
      assertThat(p2.isReady()).isTrue();
   }

   private void fireLastScheduledTimeout() {
      Runnable last = null;
      for (Runnable r : scheduledTimeouts) {
         last = r;
      }
      if (last != null) {
         last.run();
      }
   }

   private ScheduledExecutorService createMockTimeoutExecutor() {
      ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
      when(executor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenAnswer(inv -> {
               scheduledTimeouts.add(inv.getArgument(0));
               ScheduledFuture<?> future = mock(ScheduledFuture.class);
               when(future.isDone()).thenReturn(true);
               return future;
            });
      return executor;
   }

   @SuppressWarnings("unchecked")
   private static TxInvocationContext<?> context(GlobalTransaction gtx) {
      TxInvocationContext<?> ctx = mock(TxInvocationContext.class);
      when(ctx.getGlobalTransaction()).thenReturn(gtx);
      when(ctx.isOriginLocal()).thenReturn(false);
      return ctx;
   }

   private static GlobalTransaction newGlobalTransaction() {
      return new GlobalTransaction(null, false);
   }

   private static CacheTransaction pendingTransaction(GlobalTransaction gtx, Object key,
                                                       CompletableFuture<Void> releaseFuture) {
      return transactionAt(gtx, OLD_TOPOLOGY, key, releaseFuture);
   }

   private static CacheTransaction transactionAt(GlobalTransaction gtx, int topologyId, Object key,
                                                   CompletableFuture<Void> releaseFuture) {
      CacheTransaction tx = Mockito.mock(CacheTransaction.class);
      when(tx.getGlobalTransaction()).thenReturn(gtx);
      when(tx.getTopologyId()).thenReturn(topologyId);
      when(tx.getReleaseFutureForKey(key)).thenReturn(releaseFuture);
      when(tx.getLockedKeys()).thenReturn(Collections.singleton(key));
      return tx;
   }
}
