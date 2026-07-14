package org.infinispan.util.concurrent.locks.impl;

import static org.infinispan.commons.util.Util.prettyPrintTime;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.locks.PendingLockListener;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.PendingLockPromise;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default implementation for {@link PendingLockManager}.
 * <p>
 * In transactional caches, a transaction would wait for transaction originated in a older topology id. It can happen
 * when topology changes and a backup owner becomes the primary owner.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultPendingLockManager implements PendingLockManager {

   private static final Object SWEEP_MARKER = new Object();

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final int NO_PENDING_CHECK = -2;
   @Inject TransactionTable transactionTable;
   @Inject TimeService timeService;
   @Inject DistributionManager distributionManager;
   @Inject @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;

   private final Map<Object, SharedPendingSignal> activeSignals = new ConcurrentHashMap<>();
   private final AtomicReference<Object> activeSignalCleaner = new AtomicReference<>();
   private volatile int highestTopologyId;

   private final SharedPendingSignal EMPTY = new SharedPendingSignal(Collections.emptyList()) {
      @Override
      public boolean isDone() {
         return true;
      }
   };

   public DefaultPendingLockManager() { }

   @Stop
   protected void stop() {
      Object curr = activeSignalCleaner.getAndSet(SWEEP_MARKER);
      if (curr instanceof ScheduledFuture<?> f)
         f.cancel(true);
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId == NO_PENDING_CHECK) {
         if (log.isTraceEnabled()) {
            log.tracef("Skipping pending transactions check for transaction %s", globalTransaction);
         }
         return PendingLockPromise.NO_OP;
      }

      clearCompletedSignals(txTopologyId);

      SharedPendingSignal signal = activeSignals.get(key);

      // Already marked that there are no pending TXs for the given key.
      // There is no need to keep scanning the transaction table anymore, we can proceed.
      if (signal == EMPTY)
         return PendingLockPromise.NO_OP;

      if (signal != null && signal.isDone()) {
         activeSignals.remove(key, signal);
         signal = null;
      }

      if (signal == null) {
         signal = activeSignals.computeIfAbsent(key, k-> {
            Collection<PendingTransaction> transactions = getTransactionWithLockedKey(txTopologyId, k, globalTransaction);
            if (transactions.isEmpty())
               return EMPTY;

            return createSharedSignal(transactions);
         });

         if (signal != EMPTY) {
            final SharedPendingSignal s = signal;
            signal.onComplete(() -> activeSignals.remove(key, s));
         } else {
            trySchedulingSignalCleanup();
         }
      }

      if (signal == EMPTY || signal.isDone() || signal.isOnlyPendingFor(globalTransaction)) {
         return PendingLockPromise.NO_OP;
      }

      PendingLockPromiseImpl pendingLockPromise = new PendingLockPromiseImpl(globalTransaction, time, unit);
      signal.addWaiter(pendingLockPromise);
      return pendingLockPromise;
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                             long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId == NO_PENDING_CHECK) {
         if (log.isTraceEnabled()) {
            log.tracef("Skipping pending transactions check for transaction %s", globalTransaction);
         }
         return PendingLockPromise.NO_OP;
      }

      if (keys.size() == 1)
         return checkPendingTransactionsForKey(ctx, keys.iterator().next(), time, unit);

      List<PendingLockPromise> pending = null;
      for (Object key : keys) {
         PendingLockPromise promise = checkPendingTransactionsForKey(ctx, key, time, unit);
         if (promise == PendingLockPromise.NO_OP)
            continue;

         if (pending == null)
            pending = new ArrayList<>();

         pending.add(promise);
      }

      if (pending == null)
         return PendingLockPromise.NO_OP;

      if (pending.size() == 1)
         return pending.get(0);

      return new CompositePendingLockPromise(pending, time, unit);
   }

   private void trySchedulingSignalCleanup() {
      if (!activeSignalCleaner.compareAndSet(null, SWEEP_MARKER))
         return;

      ScheduledFuture<?> task = timeoutExecutor.schedule(this::cleanupCompletedSignals, 30, TimeUnit.SECONDS);
      activeSignalCleaner.set(task);
   }

   private void cleanupCompletedSignals() {
      activeSignals.values().removeIf(SharedPendingSignal::isDone);
      activeSignalCleaner.set(null);
   }

   private void clearCompletedSignals(int newTopologyId) {
      if (newTopologyId <= highestTopologyId)
         return;

      highestTopologyId = newTopologyId;
      activeSignals.values().removeIf(SharedPendingSignal::isDone);
   }

   private SharedPendingSignal createSharedSignal(Collection<PendingTransaction> transactions) {
      SharedPendingSignal signal = new SharedPendingSignal(transactions);
      signal.registerListeners();
      return signal;
   }

   private int getTopologyId(TxInvocationContext<?> context) {
      final CacheTransaction tx = context.getCacheTransaction();
      boolean isFromStateTransfer = context.isOriginLocal() && ((LocalTransaction) tx).isFromStateTransfer();
      // if the transaction is from state transfer it should not wait for the backup locks of other transactions
      if (!isFromStateTransfer) {
         final int topologyId = distributionManager.getCacheTopology().getTopologyId();
         if (topologyId != TransactionTable.CACHE_STOPPED_TOPOLOGY_ID) {
            if (transactionTable.getMinTopologyId() < topologyId) {
               return topologyId;
            }
         }
      }
      return NO_PENDING_CHECK;
   }

   private static TimeoutException timeout(KeyValuePair<CacheTransaction, Object> lockOwner,
                                           GlobalTransaction thisGlobalTransaction, long timeout, TimeUnit timeUnit) {
      return log.unableToAcquireLock(prettyPrintTime(timeout, timeUnit), lockOwner.getValue(), thisGlobalTransaction,
                                     lockOwner.getKey().getGlobalTransaction() + " (pending)", false);
   }

   private Collection<PendingTransaction> getTransactionWithLockedKey(int transactionTopologyId, Object key,
                                                                      GlobalTransaction globalTransaction) {
      if (key == null) {
         return Collections.emptyList();
      }
      final Collection<PendingTransaction> pendingTransactions = new ArrayList<>();
      forEachTransaction(transaction -> {
         if (transaction.getTopologyId() < transactionTopologyId &&
               !transaction.getGlobalTransaction().equals(globalTransaction)) {
            CompletableFuture<Void> keyReleasedFuture = transaction.getReleaseFutureForKey(key);
            if (keyReleasedFuture != null && !keyReleasedFuture.isDone()) {
               pendingTransactions.add(new PendingTransaction(transaction, Collections.singletonMap(key, keyReleasedFuture)));
            }
         }
      });
      return pendingTransactions.isEmpty() ? Collections.emptyList() : pendingTransactions;
   }

   private void forEachTransaction(Consumer<CacheTransaction> consumer) {
      final Collection<? extends CacheTransaction> localTransactions = transactionTable.getLocalTransactions();
      final Collection<? extends CacheTransaction> remoteTransactions = transactionTable.getRemoteTransactions();
      final int totalSize = localTransactions.size() + remoteTransactions.size();
      if (totalSize == 0) {
         return;
      }

      if (!localTransactions.isEmpty()) {
         localTransactions.forEach(consumer);
      }
      if (!remoteTransactions.isEmpty()) {
         remoteTransactions.forEach(consumer);
      }
   }

   private static class PendingTransaction {
      private final CacheTransaction cacheTransaction;
      private final Map<Object, CompletableFuture<Void>> keyReleased;

      private PendingTransaction(CacheTransaction cacheTransaction, Map<Object, CompletableFuture<Void>> keyReleased) {
         this.cacheTransaction = cacheTransaction;
         this.keyReleased = keyReleased;
      }

      @Override
      public String toString() {
         return "PendingTransaction{" +
               "gtx=" + cacheTransaction.getGlobalTransaction().globalId() +
               ", keys=" + keyReleased.keySet() +
               '}';
      }

      void afterCompleted(Runnable runnable) {
         for (CompletableFuture<Void> cf : keyReleased.values()) {
            if (!cf.isDone()) {
               cf.thenRun(runnable);
            }
         }

         runnable.run();
      }

      KeyValuePair<CacheTransaction, Object> findUnreleasedKey() {
         for (Map.Entry<Object, CompletableFuture<Void>> entry : keyReleased.entrySet()) {
            if (!entry.getValue().isDone()) {
               return new KeyValuePair<>(cacheTransaction, entry.getKey());
            }
         }
         return null;
      }
   }

   private class SharedPendingSignal implements Runnable {

      private final Collection<PendingTransaction> pendingTransactions;
      private final Queue<PendingLockPromiseImpl> waiters;

      // All the values are read/written by different "requester" threads and by the timeout cleanup.
      private volatile Runnable onComplete = null;
      private volatile ScheduledFuture<?> timeoutTask;
      private volatile boolean completed;

      public SharedPendingSignal(Collection<PendingTransaction> pendingTransactions) {
         this.pendingTransactions = pendingTransactions;
         this.waiters = new ConcurrentLinkedDeque<>();
      }

      public void addWaiter(PendingLockPromiseImpl waiter) {
         waiters.add(waiter);
         if (completed) {
            waiter.complete();
            return;
         }
         scheduleNextTimeout();
      }

      public void onComplete(Runnable runnable) {
         this.onComplete = runnable;
         if (isDone())
            onComplete.run();
      }

      private void scheduleNextTimeout() {
         // Waiters have the same timeout and arrive in any order.
         // The first one to arrive is most likely one to timeout next.
         // We schedule a single timeout for the first one, then we timeout everything until the un-expired waiter.
         PendingLockPromiseImpl head = waiters.peek();
         if (head == null)
            return;

         ScheduledFuture<?> curr = timeoutTask;
         if (curr != null && !curr.isDone())
            return;

         scheduleNextTimeout(head);
      }

      private void checkTimeouts() {
         if (completed)
            return;

         PendingLockPromiseImpl head;
         while ((head = waiters.peek()) != null) {

            // If not expired yet, we schedule the timeout operation only for the head and leave.
            if (!timeService.isTimeExpired(head.expectedEndTime)) {
               scheduleNextTimeout(head);
               return;
            }

            // If the head has expired, we consume the queue until finding the first non-expired entry.
            waiters.poll();
            if (!head.notifier.isDone()) {
               KeyValuePair<CacheTransaction, Object> unreleased = findUnreleasedKey();
               if (unreleased != null) {
                  head.timeout(unreleased);
               } else {
                  head.complete();
               }
            }
         }
      }

      public boolean isOnlyPendingFor(GlobalTransaction gtx) {
         for (PendingTransaction tx : pendingTransactions) {
            if (tx.findUnreleasedKey() != null && !Objects.equals(gtx, tx.cacheTransaction.getGlobalTransaction()))
               return false;
         }

         return true;
      }

      private void scheduleNextTimeout(PendingLockPromiseImpl head) {
         long remaining = timeService.remainingTime(head.expectedEndTime, TimeUnit.NANOSECONDS);
         timeoutTask = timeoutExecutor.schedule(this::checkTimeouts, remaining, TimeUnit.NANOSECONDS);
      }

      @Override
      public void run() {
         if (completed)
            return;

         for (PendingTransaction tx : pendingTransactions) {
            if (tx.findUnreleasedKey() != null)
               return;
         }

         completed = true;

         ScheduledFuture<?> t = timeoutTask;
         if (t != null)
            t.cancel(false);

         PendingLockPromiseImpl plpi;
         while ((plpi = waiters.poll()) != null) {
            plpi.complete();
         }

         if (onComplete != null)
            onComplete.run();
      }

      public boolean isDone() {
         return completed;
      }

      public KeyValuePair<CacheTransaction, Object> findUnreleasedKey() {
         for (PendingTransaction tx : pendingTransactions) {
            KeyValuePair<CacheTransaction, Object> waiting = tx.findUnreleasedKey();
            if (waiting != null)
               return waiting;
         }

         return null;
      }

      public void registerListeners() {
         for (PendingTransaction tx : pendingTransactions) {
            tx.afterCompleted(this);
         }
      }
   }

   private class PendingLockPromiseImpl implements PendingLockPromise {

      private final GlobalTransaction globalTransaction;
      private final long timeoutNanos;
      private final long expectedEndTime;
      private final CompletableFuture<Void> notifier;

      private PendingLockPromiseImpl(GlobalTransaction globalTransaction, long timeout, TimeUnit timeUnit) {
         this.globalTransaction = globalTransaction;
         this.timeoutNanos = timeUnit.toNanos(timeout);
         this.expectedEndTime = timeService.expectedEndTime(timeoutNanos, TimeUnit.NANOSECONDS);
         this.notifier = new CompletableFuture<>();
      }

      @Override
      public InvocationStage toInvocationStage() {
         return new SimpleAsyncInvocationStage(notifier);
      }

      @Override
      public boolean isReady() {
         return notifier.isDone();
      }

      @Override
      public void addListener(PendingLockListener listener) {
         notifier.whenComplete((v, throwable) -> listener.onReady());
      }

      @Override
      public boolean hasTimedOut() {
         return notifier.isCompletedExceptionally();
      }

      @Override
      public long getRemainingTimeout() {
         return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
      }

      @Override
      public CompletionStage<Void> toCompletionStage() {
         return notifier;
      }

      public void timeout(KeyValuePair<CacheTransaction, Object> unreleased) {
         log.tracef("Timed out waiting for pending unreleased %s for TX=%s", unreleased, globalTransaction);
         notifier.completeExceptionally(DefaultPendingLockManager.timeout(unreleased, globalTransaction, timeoutNanos, TimeUnit.NANOSECONDS));
      }

      public void complete() {
         log.tracef("All pending transactions have finished for TX=%s", globalTransaction);
         notifier.complete(null);
      }
   }

   private class CompositePendingLockPromise implements PendingLockPromise {

      private final CompletableFuture<Void> notifier;
      private final long expectedEndTimeNs;

      public CompositePendingLockPromise(Collection<PendingLockPromise> promises, long time, TimeUnit unit) {
         this.expectedEndTimeNs = timeService.expectedEndTime(unit.toNanos(time), TimeUnit.NANOSECONDS);

         AggregateCompletionStage<Void> aggregate = CompletionStages.aggregateCompletionStage();
         for (PendingLockPromise promise : promises) {
            aggregate.dependsOn(promise.toCompletionStage());
         }
         this.notifier = aggregate.freeze().toCompletableFuture();
      }

      @Override
      public boolean isReady() {
         return notifier.isDone();
      }

      @Override
      public void addListener(PendingLockListener listener) {
         notifier.whenComplete((v, t) -> listener.onReady());
      }

      @Override
      public boolean hasTimedOut() {
         return notifier.isCompletedExceptionally();
      }

      @Override
      public long getRemainingTimeout() {
         return timeService.remainingTime(expectedEndTimeNs, TimeUnit.MILLISECONDS);
      }

      @Override
      public InvocationStage toInvocationStage() {
         return new SimpleAsyncInvocationStage(notifier);
      }

      @Override
      public CompletionStage<Void> toCompletionStage() {
         return notifier;
      }
   }
}
