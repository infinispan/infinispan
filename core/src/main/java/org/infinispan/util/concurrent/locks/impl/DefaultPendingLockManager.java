package org.infinispan.util.concurrent.locks.impl;

import static java.lang.String.format;
import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.KeyValuePair;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
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
public class DefaultPendingLockManager implements PendingLockManager {

   private static final Log log = LogFactory.getLog(DefaultPendingLockManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int NO_PENDING_CHECK = -2;
   private final Map<GlobalTransaction, PendingLockPromise> pendingLockPromiseMap;
   @Inject private TransactionTable transactionTable;
   @Inject private TimeService timeService;
   @Inject @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   private ScheduledExecutorService timeoutExecutor;
   @Inject private DistributionManager distributionManager;

   public DefaultPendingLockManager() {
      pendingLockPromiseMap = new ConcurrentHashMap<>();
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      if (trace) {
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));
      }
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromise existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         if (trace) {
            log.tracef("PendingLock already exists: %s", existing);
         }
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {

         return createAndStore(getTransactionWithLockedKey(txTopologyId, key, globalTransaction),
                               globalTransaction, time, unit);
      }
      return createAndStore(globalTransaction);
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                             long time, TimeUnit unit) {
      if (trace) {
         log.tracef("Checking for pending locks and then locking keys %s", toStr(keys));
      }
      final GlobalTransaction globalTransaction = ctx.getGlobalTransaction();
      PendingLockPromise existing = pendingLockPromiseMap.get(globalTransaction);
      if (existing != null) {
         if (trace) {
            log.tracef("PendingLock already exists: %s", existing);
         }
         return existing;
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return createAndStore(getTransactionWithAnyLockedKey(txTopologyId, keys, globalTransaction),
                               globalTransaction, time, unit);
      }
      return createAndStore(globalTransaction);
   }

   @Override
   public long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key,
                                              long time, TimeUnit unit) throws InterruptedException {
      final GlobalTransaction gtx = ctx.getGlobalTransaction();
      PendingLockPromise pendingLockPromise = pendingLockPromiseMap.remove(gtx);
      if (trace) {
         log.tracef("Await for pending transactions for transaction %s using %s", gtx, pendingLockPromise);
      }
      if (pendingLockPromise != null) {
         return awaitOn(pendingLockPromise, gtx, time, unit);
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForPendingLock(key, gtx, txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking key %s, no need to check for pending locks.", toStr(key));
      }
      return unit.toMillis(time);
   }

   @Override
   public long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                  long time, TimeUnit unit) throws InterruptedException {
      final GlobalTransaction gtx = ctx.getGlobalTransaction();
      PendingLockPromise pendingLockPromise = pendingLockPromiseMap.remove(gtx);
      if (trace) {
         log.tracef("Await for pending transactions for transaction %s using %s", gtx, pendingLockPromise);
      }
      if (pendingLockPromise != null) {
         return awaitOn(pendingLockPromise, gtx, time, unit);
      }
      final int txTopologyId = getTopologyId(ctx);
      if (txTopologyId != NO_PENDING_CHECK) {
         return checkForAnyPendingLocks(keys, gtx, txTopologyId, unit.toMillis(time));
      }

      if (trace) {
         log.tracef("Locking keys %s, no need to check for pending locks.", toStr(keys));
      }

      return unit.toMillis(time);
   }

   private PendingLockPromise createAndStore(Collection<PendingTransaction> transactions,
                                             GlobalTransaction globalTransaction, long time, TimeUnit unit) {
      if (transactions.isEmpty()) {
         return createAndStore(globalTransaction);
      }

      if (trace) {
         log.tracef("Transactions pending for Transaction %s are %s", globalTransaction, transactions);
      }
      PendingLockPromiseImpl pendingLockPromise = new PendingLockPromiseImpl(transactions, timeService.expectedEndTime(time, unit));
      PendingLockPromise existing = pendingLockPromiseMap.putIfAbsent(globalTransaction, pendingLockPromise);
      if (trace) {
         log.tracef("Stored PendingLock is %s", existing != null ? existing : pendingLockPromise);
      }
      if (existing != null) {
         return existing;
      }
      pendingLockPromise.registerListenerInCacheTransactions();
      if (!pendingLockPromise.isReady()) {
         // schedule(Runnable) creates an extra Callable wrapper object
         timeoutExecutor.schedule((Callable<Void>) pendingLockPromise, time, unit);
      }
      return pendingLockPromise;
   }

   private PendingLockPromise createAndStore(GlobalTransaction globalTransaction) {
      if (trace) {
         log.tracef("No transactions pending for Transaction %s", globalTransaction);
      }
      PendingLockPromise existing = pendingLockPromiseMap.putIfAbsent(globalTransaction, PendingLockPromise.NO_OP);
      if (trace) {
         log.tracef("Stored PendingLock is %s", existing != null ? existing : PendingLockPromise.NO_OP);
      }
      return existing != null ? existing : PendingLockPromise.NO_OP;
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

   private long checkForPendingLock(Object key, GlobalTransaction globalTransaction, int transactionTopologyId, long lockTimeout) throws InterruptedException {
      if (trace) {
         log.tracef("Checking for pending locks and then locking key %s", toStr(key));
      }

      final long expectedEndTime = timeService.expectedEndTime(lockTimeout, TimeUnit.MILLISECONDS);

      final Collection<PendingTransaction> pendingTransactions = getTransactionWithLockedKey(transactionTopologyId, key, globalTransaction);
      if (trace)
         log.tracef("Checking for pending locks: %s", pendingTransactions);
      final KeyValuePair<CacheTransaction, Object> lockOwner = waitForTransactionsToComplete(pendingTransactions, expectedEndTime);

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

      if (trace)
         log.tracef("Checking for pending locks: %s", pendingTransactions);

      final KeyValuePair<CacheTransaction, Object> lockOwner = waitForTransactionsToComplete(pendingTransactions, expectedEndTime);

      // Then try to acquire a lock
      if (trace) {
         log.tracef("Finished waiting for other potential lockers. Timed-Out? %b", lockOwner != null);
      }

      if (lockOwner != null) {
         timeout(lockOwner, globalTransaction);
      }

      return timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
   }

   private static void timeout(KeyValuePair<CacheTransaction, Object> lockOwner, GlobalTransaction thisGlobalTransaction) {
      throw new TimeoutException(format("Could not acquire lock on %s in behalf of transaction %s. Current owner %s.",
                                        lockOwner.getValue(), thisGlobalTransaction,
                                        lockOwner.getKey().getGlobalTransaction()));
   }

   //cache-tx and key if it timed out, otherwise null
   private KeyValuePair<CacheTransaction, Object> waitForTransactionsToComplete(Collection<PendingTransaction> transactionsToCheck,
                                                            long expectedEndTime) throws InterruptedException {
      if (transactionsToCheck.isEmpty()) {
         return null;
      }
      for (PendingTransaction tx : transactionsToCheck) {
         for (Map.Entry<Object, CompletableFuture<Void>> entry : tx.keyReleased.entrySet()) {
            long remaining = timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS);
            //CF.get(time) return TimeoutException when remaining is <= 0.
            if (!CompletableFutures.await(entry.getValue(), remaining, TimeUnit.MILLISECONDS)) {
               return new KeyValuePair<>(tx.cacheTransaction, entry.getKey());
            }
         }
      }
      return null;
   }

   private Collection<PendingTransaction> getTransactionWithLockedKey(int transactionTopologyId,
                                                                      Object key,
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
      if (pendingLockPromise.hasTimedOut()) {
         timeout(((PendingLockPromiseImpl) pendingLockPromise).getPendingTransaction(), globalTransaction);
      }
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

      private final Collection<PendingTransaction> pendingTransactions;
      private final long expectedEndTime;
      private final CompletableFuture<Void> notifier;
      private volatile KeyValuePair<CacheTransaction, Object> timedOutTransaction;

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
            KeyValuePair<CacheTransaction, Object> waiting = transaction.findUnreleasedKey();
            if (waiting != null) {
               if (timeService.remainingTime(expectedEndTime, TimeUnit.MILLISECONDS) <= 0) {
                  timedOutTransaction = waiting;
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
      public Void call() throws Exception {
         //invoked when the timeout kicks.
         onRelease();
         return null;
      }

      @Override
      public void run() {
         //invoked when the timeout kicks.
         onRelease();
      }

      private void onRelease() {
         if (isReady()) {
            notifier.complete(null);
         }
      }

      private KeyValuePair<CacheTransaction, Object> getPendingTransaction() {
         return timedOutTransaction;
      }

      private void registerListenerInCacheTransactions() {
         for (PendingTransaction transaction : pendingTransactions) {
            transaction.afterCompleted(this);
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
}
