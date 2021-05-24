package org.infinispan.util.concurrent.locks.impl;

import static org.infinispan.commons.util.Util.prettyPrintTime;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.KeyValuePair;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
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

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final int NO_PENDING_CHECK = -2;
   @Inject TransactionTable transactionTable;
   @Inject TimeService timeService;
   @Inject DistributionManager distributionManager;
   @Inject @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;

   public DefaultPendingLockManager() {
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
      return createPromise(getTransactionWithLockedKey(txTopologyId, key, globalTransaction), globalTransaction, time,
                           unit);
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
      return createPromise(getTransactionWithAnyLockedKey(txTopologyId, keys, globalTransaction), globalTransaction,
                           time, unit);
   }

   @Override
   public long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key,
                                              long time, TimeUnit unit) throws InterruptedException {
      final GlobalTransaction gtx = ctx.getGlobalTransaction();
      PendingLockPromise pendingLockPromise = checkPendingTransactionsForKey(ctx, key, time, unit);
      if (log.isTraceEnabled()) {
         log.tracef("Await for pending transactions for transaction %s using %s", gtx, pendingLockPromise);
      }
      return awaitOn(pendingLockPromise, gtx, time, unit);
   }

   @Override
   public long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                  long time, TimeUnit unit) throws InterruptedException {
      final GlobalTransaction gtx = ctx.getGlobalTransaction();
      PendingLockPromise pendingLockPromise = checkPendingTransactionsForKeys(ctx, keys, time, unit);
      if (log.isTraceEnabled()) {
         log.tracef("Await for pending transactions for transaction %s using %s", gtx, pendingLockPromise);
      }
      return awaitOn(pendingLockPromise, gtx, time, unit);
   }

   private PendingLockPromise createPromise(Collection<PendingTransaction> transactions,
                                            GlobalTransaction globalTransaction, long time, TimeUnit unit) {
      if (transactions.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("No transactions pending for transaction %s", globalTransaction);
         }
         return PendingLockPromise.NO_OP;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Transactions pending for transaction %s are %s", globalTransaction, transactions);
      }
      PendingLockPromiseImpl pendingLockPromise = new PendingLockPromiseImpl(globalTransaction, time, unit, transactions);
      pendingLockPromise.scheduleTimeoutTask();
      pendingLockPromise.registerListenerInCacheTransactions();
      return pendingLockPromise;
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
      Object owner = lockOwner.getKey().getGlobalTransaction();
      return log.unableToAcquireLock(prettyPrintTime(timeout, timeUnit), lockOwner.getValue(), thisGlobalTransaction,
                                     owner + " (pending)", owner);
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
            if (keyReleasedFuture != null) {
               pendingTransactions.add(new PendingTransaction(transaction, Collections.singletonMap(key, keyReleasedFuture)));
            }
         }
      });
      return pendingTransactions.isEmpty() ? Collections.emptyList() : pendingTransactions;
   }

   private Collection<PendingTransaction> getTransactionWithAnyLockedKey(int transactionTopologyId,
                                                                         Collection<Object> keys,
                                                                         GlobalTransaction globalTransaction) {
      if (keys.isEmpty()) {
         return Collections.emptyList();
      }
      final Collection<PendingTransaction> pendingTransactions = new ArrayList<>();
      forEachTransaction(transaction -> {
         if (transaction.getTopologyId() < transactionTopologyId &&
               !transaction.getGlobalTransaction().equals(globalTransaction)) {
            Map<Object, CompletableFuture<Void>> keyReleaseFuture = transaction.getReleaseFutureForKeys(keys);
            if (keyReleaseFuture != null) {
               pendingTransactions.add(new PendingTransaction(transaction, keyReleaseFuture));
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

   private static long awaitOn(PendingLockPromise pendingLockPromise, GlobalTransaction globalTransaction, long timeout, TimeUnit timeUnit)
         throws InterruptedException {
      if (pendingLockPromise == PendingLockPromise.NO_OP) {
         return timeUnit.toMillis(timeout);
      }
      assert pendingLockPromise instanceof PendingLockPromiseImpl;
      ((PendingLockPromiseImpl) pendingLockPromise).await();
      return pendingLockPromise.getRemainingTimeout();
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
         keyReleased.values().forEach(voidCompletableFuture -> voidCompletableFuture.thenRun(runnable));
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

   private class PendingLockPromiseImpl implements PendingLockPromise, Callable<Void>, Runnable {
      private final GlobalTransaction globalTransaction;
      private final long timeoutNanos;

      private final Collection<PendingTransaction> pendingTransactions;
      private final long expectedEndTime;
      private final CompletableFuture<Void> notifier;
      private ScheduledFuture<Void> timeoutTask;

      private PendingLockPromiseImpl(GlobalTransaction globalTransaction,
                                     long timeout, TimeUnit timeUnit,
                                     Collection<PendingTransaction> pendingTransactions) {
         this.globalTransaction = globalTransaction;
         this.timeoutNanos = timeUnit.toNanos(timeout);
         this.pendingTransactions = pendingTransactions;
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
      public Void call() throws Exception {
         //invoked when the timeout kicks.
         onRelease();
         return null;
      }

      @Override
      public void run() {
         //invoked when a pending backup lock is released
         onRelease();
      }

      private void onRelease() {
         KeyValuePair<CacheTransaction, Object> timedOutTransaction = null;
         for (PendingTransaction transaction : pendingTransactions) {
            KeyValuePair<CacheTransaction, Object> waiting = transaction.findUnreleasedKey();
            if (waiting != null) {
               // Found a pending transaction
               if (timeService.isTimeExpired(expectedEndTime)) {
                  // Timed out, complete the promise
                  timedOutTransaction = waiting;
                  break;
               } else {
                  // Not timed out, wait some more
                  return;
               }
            }
         }

         if (timeoutTask != null) {
            timeoutTask.cancel(false);
         }
         if (timedOutTransaction == null) {
            if (log.isTraceEnabled()) log.tracef("All pending transactions have finished for transaction %s", globalTransaction);
            notifier.complete(null);
         } else {
            if (log.isTraceEnabled()) log.tracef("Timed out waiting for pending transaction %s for transaction %s", timedOutTransaction, globalTransaction);
            notifier.completeExceptionally(timeout(timedOutTransaction, globalTransaction, timeoutNanos, TimeUnit.NANOSECONDS));
         }
      }

      void registerListenerInCacheTransactions() {
         for (PendingTransaction transaction : pendingTransactions) {
            transaction.afterCompleted(this);
         }
         // Maybe one of the transactions has finished or removed a backup lock before we added the listener
         onRelease();
      }

      void scheduleTimeoutTask() {
         if (!notifier.isDone()) {
            // schedule(Runnable) creates an extra Callable wrapper object
            timeoutTask = timeoutExecutor.schedule((Callable<Void>) this, timeoutNanos, TimeUnit.NANOSECONDS);
         }
      }

      void await() throws InterruptedException {
         try {
            notifier.get(timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
         } catch (ExecutionException e) {
            throw new IllegalStateException("Should never happen.", e);
         } catch (java.util.concurrent.TimeoutException e) {
            //ignore
         }
      }
   }
}
