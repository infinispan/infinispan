package org.infinispan.util.concurrent.locks.impl;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.PendingLockListener;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.PendingLockPromise;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

/**
 * The default implementation for {@link PendingLockManager}.
 * <p>
 * In transactional caches, a transaction would wait for transaction originated in a older topology id. It can happen
 * when topology changes and a backup owner becomes the primary owner.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultPendingLockManager implements PendingLockManager {

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int NO_PENDING_CHECK = -2;
   private final Map<GlobalTransaction, PendingLockPromiseImpl> pendingLockPromiseMap;
   private TransactionTable transactionTable;
   private TimeService timeService;
   private ScheduledExecutorService timeoutExecutor;

   public DefaultPendingLockManager() {
      pendingLockPromiseMap = new ConcurrentHashMap<>();
   }

   @Inject
   public void inject(TransactionTable transactionTable, TimeService timeService,
                      @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.transactionTable = transactionTable;
      this.timeService = timeService;
      this.timeoutExecutor = timeoutExecutor;
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromiseImpl existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         if (trace) {
            log.tracef("Checking for pending locks and then locking key %s", toStr(key));
         }
         return createAndStore(getTransactionWithLockedKey(txTopologyId, key, globalTransaction),
                               globalTransaction, time, unit);
      }
      return PendingLockPromise.NO_OP;
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                             long time, TimeUnit unit) {
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromiseImpl existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         if (trace) {
            log.tracef("Checking for pending locks and then locking keys %s", toStr(keys));
         }
         return createAndStore(getTransactionWithAnyLockedKey(txTopologyId, keys, globalTransaction),
                               globalTransaction, time, unit);
      }
      return PendingLockPromise.NO_OP;
   }

   @Override
   public long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key,
                                              long time, TimeUnit unit) throws InterruptedException {
      PendingLockPromiseImpl pendingLockPromise = pendingLockPromiseMap.remove(ctx.getGlobalTransaction());
      if (pendingLockPromise != null) {
         pendingLockPromise.await();
         if (pendingLockPromise.hasTimedOut()) {
            timeout(pendingLockPromise.getTimedOutTransaction(), ctx.getGlobalTransaction());
         }
         return pendingLockPromise.getRemainingTimeout();
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForPendingLock(key, ctx.getGlobalTransaction(), txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking key %s, no need to check for pending locks.", toStr(key));
      }
      return unit.toMillis(time);
   }

   @Override
   public long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                  long time, TimeUnit unit) throws InterruptedException {
      PendingLockPromiseImpl pendingLockPromise = pendingLockPromiseMap.remove(ctx.getGlobalTransaction());
      if (pendingLockPromise != null) {
         pendingLockPromise.await();
         if (pendingLockPromise.hasTimedOut()) {
            timeout(pendingLockPromise.getTimedOutTransaction(), ctx.getGlobalTransaction());
         }
         return pendingLockPromise.getRemainingTimeout();
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForAnyPendingLocks(keys, ctx.getGlobalTransaction(), txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking keys %s, no need to check for pending locks.", toStr(keys));
      }

      return unit.toMillis(time);
   }

   private PendingLockPromise createAndStore(Collection<PendingTransaction> transactions,
                                             GlobalTransaction globalTransaction, long time, TimeUnit unit) {
      if (transactions.isEmpty()) {
         return PendingLockPromise.NO_OP;
      }

      PendingLockPromiseImpl pendingLockPromise = new PendingLockPromiseImpl(transactions, timeService.expectedEndTime(time, unit));
      PendingLockPromiseImpl existing = pendingLockPromiseMap.putIfAbsent(globalTransaction, pendingLockPromise);
      if (existing != null) {
         return existing;
      }
      pendingLockPromise.registerListenerInCacheTransactions();
      if (!pendingLockPromise.isReady()) {
         timeoutExecutor.schedule(pendingLockPromise, time, unit);
      }
      return pendingLockPromise;
   }

   private int getTopologyId(TxInvocationContext<?> context) {
      final CacheTransaction tx = context.getCacheTransaction();
      boolean isFromStateTransfer = context.isOriginLocal() && ((LocalTransaction) tx).isFromStateTransfer();
      // if the transaction is from state transfer it should not wait for the backup locks of other transactions
      if (!isFromStateTransfer) {
         final int transactionTopologyId = tx.getTopologyId();
         if (transactionTopologyId != TransactionTable.CACHE_STOPPED_TOPOLOGY_ID) {
            if (transactionTable.getMinTopologyId() < transactionTopologyId) {
               return transactionTopologyId;
            }
         }
      }
      return NO_PENDING_CHECK;
   }

   private long checkForPendingLock(Object key, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace) {
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));
      }

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      final Collection<PendingTransaction> pendingTransactions = getTransactionWithLockedKey(transactionTopologyId, key, globalTransaction);
      final PendingTransaction lockOwner = waitForTransactionsToComplete(pendingTransactions, expectedEndTime);

      // Then try to acquire a lock
      if (trace) {
         log.tracef("Finished waiting for other potential lockers. Timed-Out? %b", lockOwner != null);
      }

      if (lockOwner != null) {
         timeout(lockOwner, globalTransaction);
      }

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
   }

   private long checkForAnyPendingLocks(Collection<Object> keys, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace)
         log.tracef("Checking for pending locks and then locking key %s", toStr(keys));

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      final Collection<PendingTransaction> pendingTransactions = getTransactionWithAnyLockedKey(transactionTopologyId, keys, globalTransaction);
      final PendingTransaction lockOwner = waitForTransactionsToComplete(pendingTransactions, expectedEndTime);

      // Then try to acquire a lock
      if (trace) {
         log.tracef("Finished waiting for other potential lockers. Timed-Out? %b", lockOwner != null);
      }

      if (lockOwner != null) {
         timeout(lockOwner, globalTransaction);
      }

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
   }

   private void timeout(PendingTransaction lockOwner, GlobalTransaction thisGlobalTransaction) {
      throw new TimeoutException(format("Could not acquire lock on %s in behalf of transaction %s. Current owner %s.",
                                        lockOwner.commonKey, thisGlobalTransaction,
                                        lockOwner.cacheTransaction.getGlobalTransaction()));
   }

   private PendingTransaction waitForTransactionsToComplete(Collection<PendingTransaction> transactionsToCheck,
                                                            long expectedEndTime) throws InterruptedException {
      if (transactionsToCheck.isEmpty()) {
         return null;
      }
      for (PendingTransaction tx : transactionsToCheck) {
         long remaining;
         if ((remaining = timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS)) > 0) {
            if (!tx.cacheTransaction.waitForLockRelease(remaining)) {
               return tx;
            }
         }
      }
      return null;
   }

   private Collection<PendingTransaction> getTransactionWithLockedKey(int transactionTopologyId,
                                                                      Object key,
                                                                      GlobalTransaction globalTransaction) {
      final Collection<PendingTransaction> pendingTransactions = new ArrayList<>();
      forEachTransaction(transaction -> {
         if (transaction.getTopologyId() < transactionTopologyId &&
               !transaction.getGlobalTransaction().equals(globalTransaction) &&
               transaction.containsLockOrBackupLock(key)) {
            pendingTransactions.add(new PendingTransaction(transaction, key));
         }
      });
      return pendingTransactions.isEmpty() ? Collections.emptyList() : pendingTransactions;
   }

   private Collection<PendingTransaction> getTransactionWithAnyLockedKey(int transactionTopologyId,
                                                                         Collection<Object> keys,
                                                                         GlobalTransaction globalTransaction) {
      final Collection<PendingTransaction> pendingTransactions = new ArrayList<>();
      forEachTransaction(transaction -> {
         if (transaction.getTopologyId() < transactionTopologyId &&
               !transaction.getGlobalTransaction().equals(globalTransaction)) {
            Object key = transaction.findAnyLockedOrBackupLocked(keys);
            if (key != null) {
               pendingTransactions.add(new PendingTransaction(transaction, key));
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
      public final CacheTransaction cacheTransaction;
      public final Object commonKey;

      private PendingTransaction(CacheTransaction cacheTransaction, Object commonKey) {
         this.cacheTransaction = cacheTransaction;
         this.commonKey = commonKey;
      }
   }

   private class PendingLockPromiseImpl implements PendingLockPromise, CacheTransaction.TransactionCompletedListener, Runnable {

      private final Collection<PendingTransaction> pendingTransactions;
      private final long expectedEndTime;
      private final CompletableFuture<Void> notifier;
      private volatile PendingTransaction timedOutTransaction;

      private PendingLockPromiseImpl(Collection<PendingTransaction> pendingTransactions, long expectedEndTime) {
         this.pendingTransactions = pendingTransactions;
         this.expectedEndTime = expectedEndTime;
         this.notifier = new CompletableFuture<>();
      }

      @Override
      public boolean isReady() {
         if (timedOutTransaction != null) {
            return true;
         }
         for (PendingTransaction transaction : pendingTransactions) {
            if (!transaction.cacheTransaction.areLocksReleased()) {
               if (timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS) <= 0) {
                  timedOutTransaction = transaction;
               }
               return timedOutTransaction != null;
            }
         }
         return true;
      }

      @Override
      public void addListener(PendingLockListener listener) {
         notifier.thenRun(listener::onReady);
      }

      @Override
      public boolean hasTimedOut() {
         return timedOutTransaction != null;
      }

      @Override
      public long getRemainingTimeout() {
         return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
      }

      @Override
      public void onCompletion() {
         if (isReady()) {
            notifier.complete(null);
         }
      }

      @Override
      public void run() {
         isReady();
      }

      private PendingTransaction getTimedOutTransaction() {
         return timedOutTransaction;
      }

      private void registerListenerInCacheTransactions() {
         for (PendingTransaction transaction : pendingTransactions) {
            transaction.cacheTransaction.addListener(this);
         }
      }

      private void await() throws InterruptedException {
         try {
            notifier.get(timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
         } catch (ExecutionException e) {
            throw new IllegalStateException("Should never happen.", e);
         } catch (java.util.concurrent.TimeoutException e) {
            //ignore
         }
         isReady();
      }
   }

   private interface Filter {
      Object getAnyConflictingKey(CacheTransaction transaction);
   }
}
