package org.infinispan.transaction.impl;

import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAException;

import net.jcip.annotations.GuardedBy;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.synchronization.SynchronizationAdapter;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ByteString;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Repository for {@link RemoteTransaction} and {@link org.infinispan.transaction.xa.TransactionXaAdapter}s (locally
 * originated transactions).
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Listener
public class TransactionTable implements org.infinispan.transaction.TransactionTable {
   public enum CompletedTransactionStatus {
      NOT_COMPLETED, COMMITTED, ABORTED, EXPIRED
   }

   private static final Log log = LogFactory.getLog(TransactionTable.class);
   private static final boolean trace = log.isTraceEnabled();

   public static final int CACHE_STOPPED_TOPOLOGY_ID = -1;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject private String cacheName;
   @Inject protected Configuration configuration;
   @Inject protected TransactionCoordinator txCoordinator;
   @Inject private TransactionFactory txFactory;
   @Inject protected RpcManager rpcManager;
   @Inject protected CommandsFactory commandsFactory;
   @Inject private ClusteringDependentLogic clusteringLogic;
   @Inject private CacheNotifier notifier;
   @Inject private TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   @Inject private TimeService timeService;
   @Inject private CacheManagerNotifier cacheManagerNotifier;
   @Inject protected PartitionHandlingManager partitionHandlingManager;
   @Inject @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   private ScheduledExecutorService timeoutExecutor;
   @Inject private TransactionOriginatorChecker transactionOriginatorChecker;
   @Inject private TransactionManager transactionManager;

   /**
    * minTxTopologyId is the minimum topology ID across all ongoing local and remote transactions.
    */
   private volatile int minTxTopologyId = CACHE_STOPPED_TOPOLOGY_ID;
   private volatile int currentTopologyId = CACHE_STOPPED_TOPOLOGY_ID;

   private CompletedTransactionsInfo completedTransactionsInfo;

   private boolean isPessimisticLocking;
   private boolean isTotalOrder;

   private ConcurrentMap<Transaction, LocalTransaction> localTransactions;
   private ConcurrentMap<GlobalTransaction, LocalTransaction> globalToLocalTransactions;
   private ConcurrentMap<GlobalTransaction, RemoteTransaction> remoteTransactions;
   private Lock minTopologyRecalculationLock;
   protected boolean clustered = false;
   protected volatile boolean running = false;

   @Start(priority = 9) // Start before cache loader manager
   public void start() {
      this.clustered = configuration.clustering().cacheMode().isClustered();
      this.isPessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      this.isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();

      final int concurrencyLevel = configuration.locking().concurrencyLevel();
      //use the IdentityEquivalence because some Transaction implementation does not have a stable hash code function
      //and it can cause some leaks in the concurrent map.
      localTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
      globalToLocalTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);

      boolean transactional = configuration.transaction().transactionMode().isTransactional();
      if (clustered && transactional) {
         minTopologyRecalculationLock = new ReentrantLock();
         remoteTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);

         notifier.addListener(this);
         cacheManagerNotifier.addListener(this);

         boolean totalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
         if (!totalOrder) {
            completedTransactionsInfo = new CompletedTransactionsInfo();

            // Periodically run a task to cleanup the transaction table of completed transactions.
            long interval = configuration.transaction().reaperWakeUpInterval();
            timeoutExecutor.scheduleAtFixedRate(() -> completedTransactionsInfo.cleanupCompletedTransactions(),
                                                interval, interval, TimeUnit.MILLISECONDS);
            timeoutExecutor.scheduleAtFixedRate(this::cleanupTimedOutTransactions,
                                                interval, interval, TimeUnit.MILLISECONDS);
         }
      }

      running = true;
   }

   @Override
   public GlobalTransaction getGlobalTransaction(Transaction transaction) {
      if (transaction == null) {
         throw new NullPointerException("Transaction must not be null.");
      }
      LocalTransaction localTransaction = localTransactions.get(transaction);
      return localTransaction != null ? localTransaction.getGlobalTransaction() : null;
   }

   @Override
   public Collection<GlobalTransaction> getLocalGlobalTransaction() {
      return Collections.unmodifiableCollection(globalToLocalTransactions.keySet());
   }

   @Override
   public Collection<GlobalTransaction> getRemoteGlobalTransaction() {
      return Collections.unmodifiableCollection(remoteTransactions.keySet());
   }

   @Stop
   @SuppressWarnings("unused")
   private void stop() {
      running = false;
      cacheManagerNotifier.removeListener(this);
      if (clustered) {
         notifier.removeListener(this);
         currentTopologyId = CACHE_STOPPED_TOPOLOGY_ID; // indicate that the cache has stopped
      }
      shutDownGracefully();
   }

   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      //do nothing
   }

   public void localTransactionPrepared(LocalTransaction localTransaction) {
      //nothing, only used by recovery
   }

   public void enlist(Transaction transaction, LocalTransaction localTransaction) {
      if (!localTransaction.isEnlisted()) {
         SynchronizationAdapter sync =
               new SynchronizationAdapter(localTransaction, this);
         if (transactionSynchronizationRegistry != null) {
            boolean needsSuspend = false;
            try {
               // ISPN-8853: TransactionSynchronizationRegistry retrieves the running transaction from
               // thread-local context in TM, so we need to set it there if it's not set already.
               Transaction currentTransaction = transactionManager.getTransaction();
               if (currentTransaction == null) {
                  transactionManager.resume(transaction);
                  needsSuspend = true;
               } else {
                  assert currentTransaction == transaction;
               }
               transactionSynchronizationRegistry.registerInterposedSynchronization(sync);
            } catch (Exception e) {
               log.failedSynchronizationRegistration(e);
               throw new CacheException(e);
            } finally {
               if (needsSuspend) {
                  try {
                     transactionManager.suspend();
                  } catch (Exception e) {
                     throw new CacheException(e);
                  }
               }
            }

         } else {

            try {
               transaction.registerSynchronization(sync);
            } catch (Exception e) {
               log.failedSynchronizationRegistration(e);
               throw new CacheException(e);
            }
         }
         ((SyncLocalTransaction) localTransaction).setEnlisted(true);
      }
   }

   public void enlistClientTransaction(Transaction transaction, LocalTransaction localTransaction) {
      if (!localTransaction.isEnlisted()) {
         SynchronizationAdapter sync = new SynchronizationAdapter(localTransaction, this);
         try {
            transaction.registerSynchronization(sync);
         } catch (Exception e) {
            log.failedSynchronizationRegistration(e);
            throw new CacheException(e);
         }
         ((SyncLocalTransaction) localTransaction).setEnlisted(true);
      }
   }

   public void failureCompletingTransaction(Transaction tx) {
      final LocalTransaction localTransaction = localTransactions.get(tx);
      if (localTransaction != null) {
         removeLocalTransaction(localTransaction);
      }
   }

   public int getMinTopologyId() {
      return minTxTopologyId;
   }

   public void cleanupLeaverTransactions(List<Address> members) {
      // Can happen if the cache is non-transactional
      if (remoteTransactions == null)
         return;

      if (trace) log.tracef("Checking for transactions originated on leavers. Current cache members are %s, remote transactions: %d",
            members, remoteTransactions.size());
      HashSet<Address> membersSet = new HashSet<>(members);
      List<GlobalTransaction> toKill = new ArrayList<>();
      for (Map.Entry<GlobalTransaction, RemoteTransaction> e : remoteTransactions.entrySet()) {
         GlobalTransaction gt = e.getKey();
         if (trace) log.tracef("Checking transaction %s", gt);
         if (transactionOriginatorChecker.isOriginatorMissing(gt, membersSet)) {
            toKill.add(gt);
         }
      }

      if (toKill.isEmpty()) {
         if (trace) log.tracef("No remote transactions pertain to originator(s) who have left the cluster.");
      } else {
         log.debugf("The originating node left the cluster for %d remote transactions", toKill.size());
         for (GlobalTransaction gtx : toKill) {
            if (partitionHandlingManager.canRollbackTransactionAfterOriginatorLeave(gtx)) {
               log.debugf("Rolling back transaction %s because originator %s left the cluster", gtx, gtx.getAddress());
               killTransaction(gtx);
            } else {
               log.debugf("Keeping transaction %s after the originator %s left the cluster.", gtx, gtx.getAddress());
            }
         }

         if (trace) log.tracef("Completed cleaning transactions originating on leavers. Remote transactions remaining: %d",
                               remoteTransactions.size());
      }
   }

   private void cleanupTimedOutTransactions() {
      if (trace) log.tracef("About to cleanup remote transactions older than %d ms", configuration.transaction().completedTxTimeout());
      long beginning = timeService.time();
      long cutoffCreationTime = beginning - TimeUnit.MILLISECONDS.toNanos(configuration.transaction().completedTxTimeout());
      List<GlobalTransaction> toKill = new ArrayList<>();

      // Check remote transactions.
      for(Map.Entry<GlobalTransaction, RemoteTransaction> e : remoteTransactions.entrySet()) {
         GlobalTransaction gtx = e.getKey();
         RemoteTransaction remoteTx = e.getValue();
         if(remoteTx != null) {
            if (trace) log.tracef("Checking transaction %s", gtx);
            // Check the time.
            if (remoteTx.getCreationTime() - cutoffCreationTime < 0) {
               long duration = timeService.timeDuration(remoteTx.getCreationTime(), beginning, TimeUnit.MILLISECONDS);
               log.remoteTransactionTimeout(gtx, duration);
               toKill.add(gtx);
            }
         }
      }

      // Rollback the orphaned transactions and release any held locks.
      toKill.forEach(this::killTransaction);
   }

   private void killTransaction(GlobalTransaction gtx) {
      RollbackCommand rc = new RollbackCommand(ByteString.fromString(cacheName), gtx);
      commandsFactory.initializeReplicableCommand(rc, false);
      try {
         rc.invoke();
         if (trace) log.tracef("Rollback of transaction %s complete.", gtx);
      } catch (Throwable e) {
         log.unableToRollbackGlobalTx(gtx, e);
      }
   }

   /**
    * Returns the {@link RemoteTransaction} associated with the supplied transaction id. Returns null if no such
    * association exists.
    */
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      return remoteTransactions.get(txId);
   }

   public void remoteTransactionRollback(GlobalTransaction gtx) {
      final RemoteTransaction remove = removeRemoteTransaction(gtx);
      if (trace) log.tracef("Removed local transaction %s? %b", gtx, remove);
   }

   /**
    * Returns an existing remote transaction or creates one if none exists.
    * Atomicity: this method supports concurrent invocations, guaranteeing that all threads will see the same
    * transaction object.
    */
   // TODO: consider returning null instead of throwing exception when the transaction is already completed
   public RemoteTransaction getOrCreateRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications) {
      return getOrCreateRemoteTransaction(globalTx, modifications, currentTopologyId);
   }

   private RemoteTransaction getOrCreateRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications, int topologyId) {
      RemoteTransaction existingTransaction = remoteTransactions.get(globalTx);
      if (existingTransaction != null)
         return existingTransaction;

      if (!running) {
         // Assume that we wouldn't get this far if the cache was already stopped
         throw log.cacheIsStopping(cacheName);
      }

      int viewId = rpcManager.getTransport().getViewId();
      if (transactionOriginatorChecker.isOriginatorMissing(globalTx, rpcManager.getTransport().getMembers())) {
         throw log.remoteTransactionOriginatorNotInView(globalTx);
      }

      RemoteTransaction newTransaction = modifications == null ? txFactory.newRemoteTransaction(globalTx, topologyId)
            : txFactory.newRemoteTransaction(modifications, globalTx, topologyId);
      RemoteTransaction remoteTransaction = remoteTransactions.compute(globalTx, (gtx, existing) -> {
         if (existing != null) {
            if (trace)
               log.tracef("Remote transaction already registered: %s", existing);
            return existing;
         } else {
            if (isTransactionCompleted(gtx)) {
               throw log.remoteTransactionAlreadyCompleted(gtx);
            }
            if (trace)
               log.tracef("Created and registered remote transaction %s", newTransaction);
            if (topologyId < minTxTopologyId) {
               if (trace)
                  log.tracef("Changing minimum topology ID from %d to %d", minTxTopologyId, topologyId);
               minTxTopologyId = topologyId;
            }
            return newTransaction;
         }
      });

      if (rpcManager.getTransport().getViewId() != viewId &&
            transactionOriginatorChecker.isOriginatorMissing(globalTx, rpcManager.getTransport().getMembers())) {
         // Either cleanupLeaverTransactions didn't run for this view yet, or it missed the transaction we just created.
         // Kill the transaction here if necessary, but return normally, as if the cleanup task did it.
         if (partitionHandlingManager.canRollbackTransactionAfterOriginatorLeave(globalTx)) {
            log.debugf("Rolling back transaction %s because originator %s left the cluster", globalTx, globalTx.getAddress());
            killTransaction(globalTx);
         }
         return remoteTransaction;
      }

      return remoteTransaction;
   }

   /**
    * Returns the {@link org.infinispan.transaction.xa.TransactionXaAdapter} corresponding to the supplied transaction.
    * If none exists, will be created first.
    */
   public LocalTransaction getOrCreateLocalTransaction(Transaction transaction, boolean implicitTransaction) {
      return getOrCreateLocalTransaction(transaction, implicitTransaction, this::newGlobalTransaction);
   }

   /**
    * Similar to {@link #getOrCreateLocalTransaction(Transaction, boolean)} but with a custom global transaction factory.
    */
   public LocalTransaction getOrCreateLocalTransaction(Transaction transaction, boolean implicitTransaction, Supplier<GlobalTransaction> gtxFactory) {
      LocalTransaction current = localTransactions.get(transaction);
      if (current == null) {
         if (!running) {
            // Assume that we wouldn't get this far if the cache was already stopped
            throw log.cacheIsStopping(cacheName);
         }
         GlobalTransaction tx = gtxFactory.get();
         current = txFactory.newLocalTransaction(transaction, tx, implicitTransaction, currentTopologyId);
         if (trace) log.tracef("Created a new local transaction: %s", current);
         localTransactions.put(transaction, current);
         globalToLocalTransactions.put(current.getGlobalTransaction(), current);
         notifier.notifyTransactionRegistered(tx, true);
      }
      return current;
   }

   /**
    * Removes the {@link org.infinispan.transaction.xa.TransactionXaAdapter} corresponding to the given tx. Returns true
    * if such an tx exists.
    */
   public boolean removeLocalTransaction(LocalTransaction localTransaction) {
      return localTransaction != null && (removeLocalTransactionInternal(localTransaction.getTransaction()) != null);
   }

   private GlobalTransaction newGlobalTransaction() {
      Address localAddress = rpcManager != null ? rpcManager.getTransport().getAddress() : null;
      return txFactory.newGlobalTransaction(localAddress, false);
   }

   private LocalTransaction removeLocalTransactionInternal(Transaction tx) {
      LocalTransaction localTx = localTransactions.get(tx);
      if (localTx != null) {
         globalToLocalTransactions.remove(localTx.getGlobalTransaction());
         localTransactions.remove(tx);
         releaseResources(localTx);
      }
      return localTx;
   }

   private void releaseResources(CacheTransaction cacheTransaction) {
      if (cacheTransaction != null) {
         if (clustered) {
            recalculateMinTopologyIdIfNeeded(cacheTransaction);
         }
         if (trace) log.tracef("Removed %s from transaction table.", cacheTransaction);
         cacheTransaction.notifyOnTransactionFinished();
      }
   }

   /**
    * Removes the {@link RemoteTransaction} corresponding to the given tx.
    */
   public void remoteTransactionCommitted(GlobalTransaction gtx, boolean onePc) {
      boolean optimisticWih1Pc = onePc && (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC);
      if (configuration.transaction().transactionProtocol().isTotalOrder() || optimisticWih1Pc) {
         removeRemoteTransaction(gtx);
      }
   }

   public final RemoteTransaction removeRemoteTransaction(GlobalTransaction txId) {
      ByRef<RemoteTransaction> removed = new ByRef<>(null);
      // we need to mark the transaction inside compute() even if it does not exist
      remoteTransactions.compute(txId, (gtx, remoteTx) -> {
         boolean successful = remoteTx != null && !remoteTx.isMarkedForRollback();
         markTransactionCompleted(gtx, successful);
         removed.set(remoteTx);
         return null;
      });
      if (trace) log.tracef("Removed remote transaction %s ? %s", txId, removed.get());
      releaseResources(removed.get());
      return removed.get();
   }

   public int getRemoteTxCount() {
      return remoteTransactions.size();
   }

   public int getLocalTxCount() {
      return localTransactions.size();
   }

   /**
    * Looks up a LocalTransaction given a GlobalTransaction.
    * @param txId the global transaction identifier
    * @return the LocalTransaction or null if not found
    */
   public LocalTransaction getLocalTransaction(GlobalTransaction txId) {
      return globalToLocalTransactions.get(txId);
   }

   public boolean containsLocalTx(GlobalTransaction globalTransaction) {
      return globalToLocalTransactions.containsKey(globalTransaction);
   }

   public LocalTransaction getLocalTransaction(Transaction tx) {
      return localTransactions.get(tx);
   }

   public boolean containRemoteTx(GlobalTransaction globalTransaction) {
      return remoteTransactions.containsKey(globalTransaction);
   }

   public Collection<RemoteTransaction> getRemoteTransactions() {
      return remoteTransactions.values();
   }

   public Collection<LocalTransaction> getLocalTransactions() {
      return localTransactions.values();
   }

   protected final void recalculateMinTopologyIdIfNeeded(CacheTransaction removedTransaction) {
      if (removedTransaction == null) throw new IllegalArgumentException("Transaction cannot be null!");
      if (currentTopologyId != CACHE_STOPPED_TOPOLOGY_ID) {

         // Assume that we only get here if we are clustered.
         int removedTransactionTopologyId = removedTransaction.getTopologyId();
         if (removedTransactionTopologyId < minTxTopologyId) {
            if (trace) log.tracef("A transaction has a topology ID (%s) that is smaller than the smallest transaction topology ID (%s) this node knows about!  This can happen if a concurrent thread recalculates the minimum topology ID after the current transaction has been removed from the transaction table.", removedTransactionTopologyId, minTxTopologyId);
         } else if (removedTransactionTopologyId == minTxTopologyId && removedTransactionTopologyId < currentTopologyId) {
            // We should only need to re-calculate the minimum topology ID if the transaction being completed
            // has the same ID as the smallest known transaction ID, to check what the new smallest is, and this is
            // not the current topology ID.
            calculateMinTopologyId(removedTransactionTopologyId);
         }
      }
   }

   @TopologyChanged
   @SuppressWarnings("unused")
   public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
      // don't do anything if this cache is not clustered
      if (clustered) {
         if (!tce.isPre()) {
            // The topology id must be updated after the topology was updated in StateConsumerImpl,
            // as state transfer requires transactions not sent to the new owners to have a smaller topology id.
            currentTopologyId = tce.getNewTopologyId();
            log.debugf("Topology changed, recalculating minTopologyId");
            calculateMinTopologyId(-1);
         }
      }
   }

   @ViewChanged
   public void onViewChange(final ViewChangedEvent e) {
         timeoutExecutor.submit((Callable<Void>) () -> {
            cleanupLeaverTransactions(e.getNewMembers());
            return null;
         });
   }

   /**
    * This method calculates the minimum topology ID known by the current node.  This method is only used in a clustered
    * cache, and only invoked when either a topology change is detected, or a transaction whose topology ID is not the same as
    * the current topology ID.
    * <p/>
    * This method is guarded by minTopologyRecalculationLock to prevent concurrent updates to the minimum topology ID field.
    *
    * @param idOfRemovedTransaction the topology ID associated with the transaction that triggered this recalculation, or -1
    *                               if triggered by a topology change event.
    */
   @GuardedBy("minTopologyRecalculationLock")
   private void calculateMinTopologyId(int idOfRemovedTransaction) {
      minTopologyRecalculationLock.lock();
      try {
         // We should only need to re-calculate the minimum topology ID if the transaction being completed
         // has the same ID as the smallest known transaction ID, to check what the new smallest is.  We do this check
         // again here, since this is now within a synchronized method.
         if (idOfRemovedTransaction == -1 ||
               (idOfRemovedTransaction == minTxTopologyId && idOfRemovedTransaction < currentTopologyId)) {
            int minTopologyIdFound = currentTopologyId;

            for (CacheTransaction ct : localTransactions.values()) {
               int topologyId = ct.getTopologyId();
               if (topologyId < minTopologyIdFound) minTopologyIdFound = topologyId;
            }
            for (CacheTransaction ct : remoteTransactions.values()) {
               int topologyId = ct.getTopologyId();
               if (topologyId < minTopologyIdFound) minTopologyIdFound = topologyId;
            }
            if (minTopologyIdFound != minTxTopologyId) {
               if (trace) log.tracef("Changing minimum topology ID from %s to %s", minTxTopologyId, minTopologyIdFound);
               minTxTopologyId = minTopologyIdFound;
            } else {
               if (trace) log.tracef("Minimum topology ID still is %s; nothing to change", minTopologyIdFound);
            }
         }
      } finally {
         minTopologyRecalculationLock.unlock();
      }
   }

   private void shutDownGracefully() {
      if (log.isDebugEnabled())
         log.debugf("Wait for on-going transactions to finish for %s.", Util.prettyPrintTime(configuration.transaction().cacheStopTimeout(), TimeUnit.MILLISECONDS));
      // We can't use TimeService for this: some tests replace the time service so that it never
      // advances by cacheStopTimeout milliseconds.
      long now = System.nanoTime();
      long failTime = now + TimeUnit.MILLISECONDS.toNanos(configuration.transaction().cacheStopTimeout());

      boolean localTxsOnGoing = !localTransactions.isEmpty();
      while (localTxsOnGoing && System.nanoTime() - failTime < 0) {
         try {
            Thread.sleep(30);
            localTxsOnGoing = !localTransactions.isEmpty();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debugf("Interrupted waiting for %d on-going local transactions to finish.", localTransactions.size());
         }
      }

      if (remoteTransactions != null) {
         Future<?> remoteTxsFuture = timeoutExecutor.submit(() -> {
            for (RemoteTransaction tx : remoteTransactions.values()) {
               // By synchronizing on the transaction we are waiting for in-progress commands affecting
               // this transaction (and synchronizing on it in TransactionSynchronizerInterceptor).
               //noinspection SynchronizationOnLocalVariableOrMethodParameter
               synchronized (tx) {
                  // Don't actually roll back the transaction, it would just delay the shutdown
                  tx.markForRollback(true);
               }
               if (Thread.currentThread().isInterrupted())
                  break;
            }
         });

         try {
            remoteTxsFuture.get(failTime - System.nanoTime(), TimeUnit.NANOSECONDS);
         } catch (InterruptedException e) {
            log.debug("Interrupted waiting for on-going remote transactional commands to finish.");
            remoteTxsFuture.cancel(true);
            Thread.currentThread().interrupt();
         } catch (ExecutionException e) {
            log.debug("Exception while waiting for on-going remote transactional commands to finish", e);
         } catch (TimeoutException e) {
            remoteTxsFuture.cancel(true);
         }
      }

      if (!localTransactions.isEmpty() || remoteTransactionsCount() != 0) {
         log.unfinishedTransactionsRemain(localTransactions.size(), remoteTransactionsCount());
         if (trace) {
            log.tracef("Unfinished local transactions: %s",
                  localTransactions.values().stream().map(tx -> tx.getGlobalTransaction().toString())
                        .collect(Collectors.joining(", ", "[", "]")));

            log.tracef("Unfinished remote transactions: %s",
                  remoteTransactions == null ? "none" : remoteTransactions.keySet());
         }
      } else {
         log.debug("All transactions terminated");
      }
   }

   private int remoteTransactionsCount() {
      return remoteTransactions == null ? 0 : remoteTransactions.size();
   }

   /**
    * With the current state transfer implementation it is possible for a transaction to be prepared several times
    * on a remote node. This might cause leaks, e.g. if the transaction is prepared, committed and prepared again.
    * Once marked as completed (because of commit or rollback) any further prepare received on that transaction are discarded.
    */
   public void markTransactionCompleted(GlobalTransaction gtx, boolean successful) {
      if (completedTransactionsInfo != null) {
         completedTransactionsInfo.markTransactionCompleted(gtx, successful);
      }
   }

   /**
    * @see #markTransactionCompleted(org.infinispan.transaction.xa.GlobalTransaction, boolean)
    */
   public boolean isTransactionCompleted(GlobalTransaction gtx) {
      return completedTransactionsInfo != null && completedTransactionsInfo.isTransactionCompleted(gtx);

   }

   /**
    * @see #markTransactionCompleted(org.infinispan.transaction.xa.GlobalTransaction, boolean)
    */
   public CompletedTransactionStatus getCompletedTransactionStatus(GlobalTransaction gtx) {
      if (completedTransactionsInfo == null)
         return CompletedTransactionStatus.NOT_COMPLETED;

      return completedTransactionsInfo.getTransactionStatus(gtx);
   }

   private class CompletedTransactionsInfo {
      final ConcurrentMap<GlobalTransaction, CompletedTransactionInfo> completedTransactions;
      // The ConcurrentMap transaction id previously cleared, one per originator
      final ConcurrentMap<Address, Long> nodeMaxPrunedTxIds;
      // The highest transaction id previously cleared, with any originator
      volatile long globalMaxPrunedTxId;

      CompletedTransactionsInfo() {
         nodeMaxPrunedTxIds = new ConcurrentHashMap<>();
         completedTransactions = new ConcurrentHashMap<>();
         globalMaxPrunedTxId = -1;
      }


      /**
       * With the current state transfer implementation it is possible for a transaction to be prepared several times
       * on a remote node. This might cause leaks, e.g. if the transaction is prepared, committed and prepared again.
       * Once marked as completed (because of commit or rollback) any further prepare received on that transaction are discarded.
       */
      void markTransactionCompleted(GlobalTransaction globalTx, boolean successful) {
         if (trace) log.tracef("Marking transaction %s as completed", globalTx);
         completedTransactions.put(globalTx, new CompletedTransactionInfo(timeService.time(), successful));
      }

      /**
       * @see #markTransactionCompleted(GlobalTransaction, boolean)
       */
      boolean isTransactionCompleted(GlobalTransaction gtx) {
         if (completedTransactions.containsKey(gtx))
            return true;

         // Transaction ids are allocated in sequence, so any transaction with a smaller id must have been started
         // before a transaction that was already removed from the completed transactions map because it was too old.
         // We assume that the transaction was either committed, or it was rolled back (e.g. because the prepare
         // RPC timed out.
         // Note: We must check the id *after* verifying that the tx doesn't exist in the map.
         if (gtx.getId() > globalMaxPrunedTxId)
            return false;
         Long nodeMaxPrunedTxId = nodeMaxPrunedTxIds.get(gtx.getAddress());
         return nodeMaxPrunedTxId != null && gtx.getId() <= nodeMaxPrunedTxId;
      }

      CompletedTransactionStatus getTransactionStatus(GlobalTransaction gtx) {
         CompletedTransactionInfo completedTx = completedTransactions.get(gtx);
         if (completedTx != null) {
            return completedTx.successful ? CompletedTransactionStatus.COMMITTED : CompletedTransactionStatus.ABORTED;
         }

         // Transaction ids are allocated in sequence, so any transaction with a smaller id must have been started
         // before a transaction that was already removed from the completed transactions map because it was too old.
         // We assume that the transaction was either committed, or it was rolled back (e.g. because the prepare
         // RPC timed out.
         // Note: We must check the id *after* verifying that the tx doesn't exist in the map.
         if (gtx.getId() > globalMaxPrunedTxId)
            return CompletedTransactionStatus.NOT_COMPLETED;
         Long nodeMaxPrunedTxId = nodeMaxPrunedTxIds.get(gtx.getAddress());
         if (nodeMaxPrunedTxId == null) {
            // We haven't removed any transaction for this node
            return CompletedTransactionStatus.NOT_COMPLETED;
         } else if (gtx.getId() > nodeMaxPrunedTxId) {
            // We haven't removed this particular transaction yet
            return CompletedTransactionStatus.NOT_COMPLETED;
         } else {
            // We already removed the status of this transaction from the completed transactions map
            return CompletedTransactionStatus.EXPIRED;
         }
      }

      void cleanupCompletedTransactions() {
         if (completedTransactions.isEmpty())
            return;

         try {
            if (trace) log.tracef("About to cleanup completed transaction. Initial size is %d", completedTransactions.size());
            long beginning = timeService.time();
            long minCompleteTimestamp = timeService.time() - TimeUnit.MILLISECONDS.toNanos(configuration.transaction().completedTxTimeout());
            int removedEntries = 0;

            // Collect the leavers. They will be removed at the end.
            Set<Address> leavers = new HashSet<>();
            for (Map.Entry<Address, Long> e : nodeMaxPrunedTxIds.entrySet()) {
               if (!rpcManager.getMembers().contains(e.getKey())) {
                  leavers.add(e.getKey());
               }
            }

            // Remove stale completed transactions.
            Iterator<Map.Entry<GlobalTransaction, CompletedTransactionInfo>> txIterator = completedTransactions.entrySet().iterator();
            while (txIterator.hasNext()) {
               Map.Entry<GlobalTransaction, CompletedTransactionInfo> e = txIterator.next();
               CompletedTransactionInfo completedTx = e.getValue();
               if (minCompleteTimestamp - completedTx.timestamp > 0) {
                  // Need to update lastPrunedTxId *before* removing the tx from the map
                  // Don't need atomic operations, there can't be more than one thread updating lastPrunedTxId.
                  final long txId = e.getKey().getId();
                  final Address address = e.getKey().getAddress();
                  updateLastPrunedTxId(txId, address);

                  txIterator.remove();
                  removedEntries++;
               } else {
                  // Nodes with "active" completed transactions are not removed..
                  leavers.remove(e.getKey().getAddress());
               }
            }

            // Finally, remove nodes that are no longer members and don't have any "active" completed transactions.
            leavers.forEach(nodeMaxPrunedTxIds::remove);

            long duration = timeService.timeDuration(beginning, TimeUnit.MILLISECONDS);

            if (trace) log.tracef("Finished cleaning up completed transactions in %d millis, %d transactions were removed, " +
                  "current number of completed transactions is %d",
                  removedEntries, duration, completedTransactions.size());
            if (trace) log.tracef("Last pruned transaction ids were updated: %d, %s", globalMaxPrunedTxId, nodeMaxPrunedTxIds);
         } catch (Exception e) {
            log.errorf(e, "Failed to cleanup completed transactions: %s", e.getMessage());
         }
      }

      private void updateLastPrunedTxId(final long txId, Address address) {
         if (txId > globalMaxPrunedTxId) {
            globalMaxPrunedTxId = txId;
         }
         nodeMaxPrunedTxIds.compute(address, (a, nodeMaxPrunedTxId) -> {
            if (nodeMaxPrunedTxId != null && txId <= nodeMaxPrunedTxId) {
               return nodeMaxPrunedTxId;
            }
            return txId;
         });
      }
   }

   public int beforeCompletion(LocalTransaction localTransaction) {
      if (trace)
         log.tracef("beforeCompletion called for %s", localTransaction);
      try {
         txCoordinator.prepare(localTransaction);
      } catch (XAException e) {
         throw new CacheException("Could not prepare. ", e);//todo shall we just swallow this exception?
      }
      return 0;
   }

   public void afterCompletion(LocalTransaction localTransaction, int status) {
      if (trace) {
         log.tracef("afterCompletion(%s) called for %s.", (Integer) status, localTransaction);
      }
      boolean isOnePhase;
      if (status == Status.STATUS_COMMITTED) {
         try {
            isOnePhase = txCoordinator.commit(localTransaction, false);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
         releaseLocksForCompletedTransaction(localTransaction, isOnePhase);
      } else if (status == Status.STATUS_ROLLEDBACK) {
         try {
            txCoordinator.rollback(localTransaction);
         } catch (XAException e) {
            throw new CacheException("Could not commit.", e);
         }
      } else {
         throw new IllegalArgumentException("Unknown status: " + status);
      }
   }

   protected final void releaseLocksForCompletedTransaction(LocalTransaction localTransaction,
         boolean committedInOnePhase) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      removeLocalTransaction(localTransaction);
      if (trace)
         log.tracef("Committed in onePhase? %s isOptimistic? %s", committedInOnePhase, isOptimisticCache());
      if (committedInOnePhase && isOptimisticCache())
         return;
      if (isClustered()) {
         removeTransactionInfoRemotely(localTransaction, gtx);
      }
   }

   private void removeTransactionInfoRemotely(LocalTransaction localTransaction, GlobalTransaction gtx) {
      if (mayHaveRemoteLocks(localTransaction) && !partitionHandlingManager.isTransactionPartiallyCommitted(gtx)) {
         TxCompletionNotificationCommand command = commandsFactory.buildTxCompletionNotificationCommand(null, gtx);
         LocalizedCacheTopology cacheTopology = clusteringLogic.getCacheTopology();
         Collection<Address> owners =
               cacheTopology.getWriteOwners(localTransaction.getAffectedKeys());
         Collection<Address> commitNodes = cacheTopology.getReadConsistentHash().isReplicated() ? null : owners;
         commitNodes = localTransaction.getCommitNodes(commitNodes, cacheTopology);
         if (trace)
            log.tracef("About to invoke tx completion notification on commitNodes: %s", commitNodes);
         rpcManager.invokeRemotely(commitNodes, command,
               rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE));
      }
   }

   private boolean mayHaveRemoteLocks(LocalTransaction lt) {
      return !isTotalOrder &&
            (lt.getRemoteLocksAcquired() != null && !lt.getRemoteLocksAcquired().isEmpty() ||
                  !lt.getModifications().isEmpty() ||
                  isPessimisticLocking && lt.getTopologyId() != rpcManager.getTopologyId());
   }

   private boolean isClustered() {
      return rpcManager != null;
   }

   private boolean isOptimisticCache() {
      //a transactional cache that is neither total order nor pessimistic must be optimistic.
      return !isPessimisticLocking && !isTotalOrder;
   }

   private static class CompletedTransactionInfo {
      public final long timestamp;
      public final boolean successful;

      private CompletedTransactionInfo(long timestamp, boolean successful) {
         this.timestamp = timestamp;
         this.successful = successful;
      }
   }
}
