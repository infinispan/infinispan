/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import net.jcip.annotations.GuardedBy;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.synchronization.SynchronizationAdapter;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionSynchronizationRegistry;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.util.Util.currentMillisFromNanotime;

/**
 * Repository for {@link RemoteTransaction} and {@link org.infinispan.transaction.xa.TransactionXaAdapter}s (locally
 * originated transactions).
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Listener
public class TransactionTable {

   public static final int CACHE_STOPPED_TOPOLOGY_ID = -1;
   private static final Log log = LogFactory.getLog(TransactionTable.class);

   private ConcurrentMap<Transaction, LocalTransaction> localTransactions;
   private ConcurrentMap<GlobalTransaction, LocalTransaction> globalToLocalTransactions;
   private ConcurrentMap<GlobalTransaction, RemoteTransaction> remoteTransactions;

   protected Configuration configuration;
   protected InvocationContextContainer icc;
   protected TransactionCoordinator txCoordinator;
   protected TransactionFactory txFactory;
   protected RpcManager rpcManager;
   protected CommandsFactory commandsFactory;
   private InterceptorChain invoker;
   private CacheNotifier notifier;
   private TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   protected ClusteringDependentLogic clusteringLogic;
   protected boolean clustered = false;
   private Lock minTopologyRecalculationLock;
   private final ConcurrentMap<GlobalTransaction, Long> completedTransactions = CollectionFactory.makeConcurrentMap();

   private ScheduledExecutorService executorService;

   /**
    * minTxTopologyId is the minimum topology ID across all ongoing local and remote transactions.
    */
   private volatile int minTxTopologyId = CACHE_STOPPED_TOPOLOGY_ID;
   private volatile int currentTopologyId = CACHE_STOPPED_TOPOLOGY_ID;
   private String cacheName;

   @Inject
   public void initialize(RpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker, CacheNotifier notifier,
                          TransactionFactory gtf, TransactionCoordinator txCoordinator,
                          TransactionSynchronizationRegistry transactionSynchronizationRegistry,
                          CommandsFactory commandsFactory, ClusteringDependentLogic clusteringDependentLogic, Cache cache) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
      this.txFactory = gtf;
      this.txCoordinator = txCoordinator;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.commandsFactory = commandsFactory;
      this.clusteringLogic = clusteringDependentLogic;
      this.cacheName = cache.getName();
   }

   @Start(priority = 9) // Start before cache loader manager
   @SuppressWarnings("unused")
   private void start() {
      final int concurrencyLevel = configuration.locking().concurrencyLevel();
      localTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
      globalToLocalTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
      if (configuration.clustering().cacheMode().isClustered()) {
         minTopologyRecalculationLock = new ReentrantLock();
         // Only initialize this if we are clustered.
         remoteTransactions = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
         notifier.addListener(this);
         clustered = true;
      }
      
      // Periodically run a task to cleanup the transaction table from completed transactions.
      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            String address = rpcManager != null ? rpcManager.getTransport().getAddress().toString() : "local";
            Thread th = new Thread(r, "TxCleanupService," + cacheName + "," + address);
            th.setDaemon(true);
            return th;
         }
      };

      executorService = Executors.newSingleThreadScheduledExecutor(tf);

      long interval = configuration.transaction().reaperWakeUpInterval();
      executorService.scheduleAtFixedRate(new Runnable() {
         @Override
         public void run() {
            cleanupCompletedTransactions();
         }
      }, interval, interval, TimeUnit.MILLISECONDS);

   }

   @Stop
   @SuppressWarnings("unused")
   private void stop() {
      
      if (executorService != null)
         executorService.shutdownNow();
      
      if (clustered) {
         notifier.removeListener(this);
         currentTopologyId = CACHE_STOPPED_TOPOLOGY_ID; // indicate that the cache has stopped
      }
      shutDownGracefully();
   }

   public Set<Object> getLockedKeysForRemoteTransaction(GlobalTransaction gtx) {
      RemoteTransaction transaction = remoteTransactions.get(gtx);
      if (transaction == null) return InfinispanCollections.emptySet();
      return transaction.getLockedKeys();
   }


   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      //do nothing
   }

   public void localTransactionPrepared(LocalTransaction localTransaction) {
      //nothing, only used by recovery
   }

   public void enlist(Transaction transaction, LocalTransaction localTransaction) {
      if (!localTransaction.isEnlisted()) {
         SynchronizationAdapter sync = new SynchronizationAdapter(
               localTransaction, txCoordinator, commandsFactory, rpcManager,
               this, clusteringLogic, configuration);
         if (transactionSynchronizationRegistry != null) {
            try {
               transactionSynchronizationRegistry.registerInterposedSynchronization(sync);
            } catch (Exception e) {
               log.failedSynchronizationRegistration(e);
               throw new CacheException(e);
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

   public void failureCompletingTransaction(Transaction tx) {
      final LocalTransaction localTransaction = localTransactions.get(tx);
      if (localTransaction != null) {
         removeLocalTransaction(localTransaction);
      }
   }

   /**
    * Returns true if the given transaction is already registered with the transaction table.
    *
    * @param tx if null false is returned
    */
   public boolean containsLocalTx(Transaction tx) {
      return tx != null && localTransactions.containsKey(tx);
   }

   public int getMinTopologyId() {
      return minTxTopologyId;
   }

   public void cleanupStaleTransactions(CacheTopology cacheTopology) {
      int topologyId = cacheTopology.getTopologyId();
      List<Address> members = cacheTopology.getMembers();

      // We only care about transactions originated before this topology update
      if (getMinTopologyId() >= topologyId)
         return;

      log.tracef("Checking for transactions originated on leavers. Current members are %s, remote transactions: %d",
            members, remoteTransactions.size());
      Set<GlobalTransaction> toKill = new HashSet<GlobalTransaction>();
      for (Map.Entry<GlobalTransaction, RemoteTransaction> e : remoteTransactions.entrySet()) {
         GlobalTransaction gt = e.getKey();
         RemoteTransaction remoteTx = e.getValue();
         log.tracef("Checking transaction %s", gt);
         // The topology id check is needed for joiners
         if (remoteTx.getTopologyId() < topologyId && !members.contains(gt.getAddress())) {
            toKill.add(gt);
         }
      }

      if (toKill.isEmpty()) {
         log.tracef("No global transactions pertain to originator(s) who have left the cluster.");
      } else {
         log.tracef("%s global transactions pertain to leavers and need to be killed", toKill.size());
      }

      for (GlobalTransaction gtx : toKill) {
         log.tracef("Killing remote transaction originating on leaver %s", gtx);
         RollbackCommand rc = new RollbackCommand(cacheName, gtx);
         rc.init(invoker, icc, TransactionTable.this);
         try {
            rc.perform(null);
            log.tracef("Rollback of transaction %s complete.", gtx);
         } catch (Throwable e) {
            log.unableToRollbackGlobalTx(gtx, e);
         }
      }

      log.tracef("Completed cleaning transactions originating on leavers. Remote transactions remaining: %d",
            remoteTransactions.size());
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
      log.tracef("Removed local transaction %s? %b", gtx, remove);
   }

   /**
    * Returns an existing remote transaction or creates one if none exists.
    * Atomicity: this method supports concurrent invocations, guaranteeing that all threads will see the same
    * transaction object.
    */
   public RemoteTransaction getOrCreateRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications) {
      return getOrCreateRemoteTransaction(globalTx, modifications, currentTopologyId);
   }

   private RemoteTransaction getOrCreateRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications, int topologyId) {
      RemoteTransaction remoteTransaction = remoteTransactions.get(globalTx);
      if (remoteTransaction != null)
         return remoteTransaction;
      remoteTransaction = modifications == null ? txFactory.newRemoteTransaction(globalTx, topologyId)
            : txFactory.newRemoteTransaction(modifications, globalTx, topologyId);
      RemoteTransaction existing = remoteTransactions.putIfAbsent(globalTx, remoteTransaction);
      if (existing != null) {
         log.tracef("Remote transaction already registered: %s", existing);
         return existing;
      } else {
         log.tracef("Created and registered remote transaction %s", remoteTransaction);
         if (remoteTransaction.getTopologyId() < minTxTopologyId) {
            log.tracef("Changing minimum topology ID from %d to %d", minTxTopologyId, remoteTransaction.getTopologyId());
            minTxTopologyId = remoteTransaction.getTopologyId();
         }
         return remoteTransaction;
      }
   }

   /**
    * Returns the {@link org.infinispan.transaction.xa.TransactionXaAdapter} corresponding to the supplied transaction.
    * If none exists, will be created first.
    */
   public LocalTransaction getOrCreateLocalTransaction(Transaction transaction, TxInvocationContext ctx) {
      LocalTransaction current = localTransactions.get(transaction);
      if (current == null) {
         Address localAddress = rpcManager != null ? rpcManager.getTransport().getAddress() : null;
         GlobalTransaction tx = txFactory.newGlobalTransaction(localAddress, false);
         current = txFactory.newLocalTransaction(transaction, tx, ctx.isImplicitTransaction(), currentTopologyId);
         log.tracef("Created a new local transaction: %s", current);
         localTransactions.put(transaction, current);
         globalToLocalTransactions.put(current.getGlobalTransaction(), current);
         notifier.notifyTransactionRegistered(tx, ctx);
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

   protected final LocalTransaction removeLocalTransactionInternal(Transaction tx) {
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
         log.tracef("Removed %s from transaction table.", cacheTransaction);
         cacheTransaction.notifyOnTransactionFinished();
      }
   }

   /**
    * Removes the {@link RemoteTransaction} corresponding to the given tx.
    */
   public void remoteTransactionCommitted(GlobalTransaction gtx) {
      if (Configurations.isSecondPhaseAsync(configuration) || configuration.transaction().transactionProtocol().isTotalOrder()) {
         removeRemoteTransaction(gtx);
      }
   }

   public final RemoteTransaction removeRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction removed = remoteTransactions.remove(txId);
      log.tracef("Removed remote transaction %s ? %s", txId, removed);
      releaseResources(removed);
      return removed;
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
            log.tracef("A transaction has a topology ID (%s) that is smaller than the smallest transaction topology ID (%s) this node knows about!  This can happen if a concurrent thread recalculates the minimum topology ID after the current transaction has been removed from the transaction table.", removedTransactionTopologyId, minTxTopologyId);
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
         if (tce.isPre()) {
            currentTopologyId = tce.getNewTopologyId();
         } else {
            log.debugf("Topology changed, recalculating minTopologyId");
            calculateMinTopologyId(-1);
         }
      }
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
               log.tracef("Changing minimum topology ID from %s to %s", minTxTopologyId, minTopologyIdFound);
               minTxTopologyId = minTopologyIdFound;
            } else {
               log.tracef("Minimum topology ID still is %s; nothing to change", minTopologyIdFound);
            }
         }
      } finally {
         minTopologyRecalculationLock.unlock();
      }
   }

   private boolean areTxsOnGoing() {
      return !localTransactions.isEmpty() || (remoteTransactions != null && !remoteTransactions.isEmpty());
   }

   private void shutDownGracefully() {
      if (log.isDebugEnabled())
         log.debugf("Wait for on-going transactions to finish for %s.", Util.prettyPrintTime(configuration.transaction().cacheStopTimeout(), TimeUnit.MILLISECONDS));
      long failTime = currentMillisFromNanotime() + configuration.transaction().cacheStopTimeout();
      boolean txsOnGoing = areTxsOnGoing();
      while (txsOnGoing && currentMillisFromNanotime() < failTime) {
         try {
            Thread.sleep(30);
            txsOnGoing = areTxsOnGoing();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (clustered) {
               log.debugf("Interrupted waiting for on-going transactions to finish. %s local transactions and %s remote transactions", localTransactions.size(), remoteTransactions.size());
            } else {
               log.debugf("Interrupted waiting for %s on-going transactions to finish.", localTransactions.size());
            }
         }
      }

      if (txsOnGoing) {
         log.unfinishedTransactionsRemain(localTransactions == null ? 0 : localTransactions.size(),
                                          remoteTransactions == null ? 0 : remoteTransactions.size());
      } else {
         log.debug("All transactions terminated");
      }
   }

   /**
    * With the current state transfer implementation it is possible for a transaction to be prepared several times
    * on a remote node. This might cause leaks, e.g. if the transaction is prepared, committed and prepared again.
    * Once marked as completed (because of commit or rollback) any further prepare received on that transaction are discarded.
    */
   public void markTransactionCompleted(GlobalTransaction globalTx) {
      completedTransactions.put(globalTx, System.nanoTime());
   }

   /**
    * @see #markTransactionCompleted(org.infinispan.transaction.xa.GlobalTransaction)
    */
   public boolean isTransactionCompleted(GlobalTransaction gtx) {
      return completedTransactions.containsKey(gtx);
   }

   public void cleanupCompletedTransactions() {
      if (!completedTransactions.isEmpty()) {
         try {
            log.tracef("About to cleanup completed transaction. Initial size is %d", completedTransactions.size());
            //this iterator is weekly consistent and will never throw ConcurrentModificationException
            Iterator<Map.Entry<GlobalTransaction, Long>> iterator = completedTransactions.entrySet().iterator();
            long timeout = configuration.transaction().completedTxTimeout();

            int removedEntries = 0;
            long beginning = System.nanoTime();
            while (iterator.hasNext()) {
               Map.Entry<GlobalTransaction, Long> e = iterator.next();
               long ageNanos = System.nanoTime() - e.getValue();
               if (TimeUnit.NANOSECONDS.toMillis(ageNanos) >= timeout) {
                  iterator.remove();
                  removedEntries++;
               }
            }
            long duration = System.nanoTime() - beginning;

            log.tracef("Finished cleaning up completed transactions. %d transactions were removed, total duration was %d millis, " +
                  "current number of completed transactions is %d", removedEntries, TimeUnit.NANOSECONDS.toMillis(duration),
                  completedTransactions.size());
         } catch (Exception e) {
            log.errorf(e, "Failed to cleanup completed transactions: %s", e.getMessage());
         }
      }
   }
}
