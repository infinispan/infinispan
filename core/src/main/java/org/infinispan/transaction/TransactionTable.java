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

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.synchronization.SynchronizationAdapter;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionSynchronizationRegistry;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Collections.emptySet;

/**
 * Repository for {@link RemoteTransaction} and {@link
 * org.infinispan.transaction.xa.TransactionXaAdapter}s (locally originated transactions).
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Listener(sync = false)
public class TransactionTable {

   private static final Log log = LogFactory.getLog(TransactionTable.class);

   /**
    * Must stay private as removals from this collection trigger a {@link #minTxViewId} recalculation.
    */
   private final ConcurrentMap<Transaction, LocalTransaction> localTransactions = new ConcurrentHashMap<Transaction, LocalTransaction>();

   /**
    * Must stay private as removals from this collection trigger a {@link #minTxViewId} recalculation.
    */
   private final ConcurrentMap<GlobalTransaction, RemoteTransaction> remoteTransactions = new ConcurrentHashMap<GlobalTransaction, RemoteTransaction>();


   private final StaleTransactionCleanupService cleanupService = new StaleTransactionCleanupService(this);

   protected Configuration configuration;
   protected InvocationContextContainer icc;
   protected TransactionCoordinator txCoordinator;
   protected TransactionFactory txFactory;
   protected RpcManager rpcManager;
   protected CommandsFactory commandsFactory;
   private InterceptorChain invoker;
   private CacheNotifier notifier;
   private EmbeddedCacheManager cm;
   private TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   protected ClusteringDependentLogic clusteringLogic;
   private int minTxViewId = -1;

   /**
    * Invariant: minTxViewId is the minimum view id between all the local and the remote transactions. It doesn't synchronize
    * on transaction creation, but only on removal. That's because it is not possible for a newly created transaction
    * to have an bigger viewId than the current one.
    */
   protected final Object minViewIdInvariant = new Object();

   @Inject
   public void initialize(RpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker, CacheNotifier notifier,
                          TransactionFactory gtf, EmbeddedCacheManager cm, TransactionCoordinator txCoordinator,
                          TransactionSynchronizationRegistry transactionSynchronizationRegistry,
                          CommandsFactory commandsFactory, ClusteringDependentLogic clusteringDependentLogic)  {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
      this.txFactory = gtf;
      this.cm = cm;
      this.txCoordinator = txCoordinator;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.commandsFactory = commandsFactory;
      this.clusteringLogic = clusteringDependentLogic;
   }

   @Start
   private void start() {
      if (configuration.getCacheMode().isClustered()) {
         cleanupService.start(configuration, rpcManager, invoker);
         cm.addListener(cleanupService);
         cm.addListener(this);
         notifier.addListener(cleanupService);
         minTxViewId = getCurrentViewId();
         log.debugf("Min view id set to %s", minTxViewId);
      }
   }

   @Stop
   private void stop() {
      if (configuration.getCacheMode().isClustered()) {
         notifier.removeListener(cleanupService);
         cm.removeListener(cleanupService);
         cleanupService.stop();
         cm.removeListener(this);
      }
      shutDownGracefully();
   }

   public Set<Object> getLockedKeysForRemoteTransaction(GlobalTransaction gtx) {
      RemoteTransaction transaction = remoteTransactions.get(gtx);
      if (transaction == null) return emptySet();
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
         SynchronizationAdapter sync = new SynchronizationAdapter(localTransaction, txCoordinator, commandsFactory,
                                                                  rpcManager, this, clusteringLogic, configuration);
         if(transactionSynchronizationRegistry != null) {
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
    * @param tx if null false is returned
    */
   public boolean containsLocalTx(Transaction tx) {
      return tx != null && localTransactions.containsKey(tx);
   }

   public Integer getMinViewId() {
      return minTxViewId;
   }

   public Set<CacheTransaction> getTransactionsStartedBefore(Integer viewId) {
      Set<CacheTransaction> result = new HashSet<CacheTransaction>();
      for (CacheTransaction ct : localTransactions.values()) {
         if (ct.getViewId() != null && ct.getViewId() < viewId) {
            result.add(ct);
         }
      }
      for (CacheTransaction ct : remoteTransactions.values()) {
         if (ct.getViewId() != null && ct.getViewId() < viewId) {
            result.add(ct);
         }
      }
      return result;
   }

   protected void updateStateOnNodesLeaving(Collection<Address> leavers) {
      Set<GlobalTransaction> toKill = new HashSet<GlobalTransaction>();
      for (GlobalTransaction gt : remoteTransactions.keySet()) {
         if (leavers.contains(gt.getAddress())) toKill.add(gt);
      }

      if (toKill.isEmpty())
         log.tracef("No global transactions pertain to originator(s) %s who have left the cluster.", leavers);
      else
         log.tracef("%s global transactions pertain to leavers list %s and need to be killed", toKill.size(), leavers);

      for (GlobalTransaction gtx : toKill) {
         log.tracef("Killing remote transaction originating on leaver %s", gtx);
         RollbackCommand rc = new RollbackCommand(configuration.getName(), gtx);
         rc.init(invoker, icc, TransactionTable.this);
         try {
            rc.perform(null);
            log.tracef("Rollback of transaction %s complete.", gtx);
         } catch (Throwable e) {
            log.unableToRollbackGlobalTx(gtx, e);
         }
      }

      log.trace("Completed cleaning transactions originating on leavers");
   }

   /**
    * Returns the {@link RemoteTransaction} associated with the supplied transaction id.
    * Returns null if no such association exists.
    */
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      return remoteTransactions.get(txId);
   }

   public void remoteTransactionRollback(GlobalTransaction gtx) {
      final RemoteTransaction remove = removeRemoteTransaction(gtx);
      log.tracef("Removed local transaction %s? %b", gtx, remove);
   }

   /**
    * Creates and register a {@link RemoteTransaction} with no modifications. Returns the
    * created transaction.
    *
    * @throws IllegalStateException if an attempt to create a {@link RemoteTransaction}
    *                               for an already registered id is made.
    */
   public RemoteTransaction createRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications) {
      RemoteTransaction remoteTransaction = modifications == null ? txFactory.newRemoteTransaction(globalTx)
            : txFactory.newRemoteTransaction(modifications, globalTx);
      updateViewId(remoteTransaction);
      registerRemoteTransaction(globalTx, remoteTransaction);
      return remoteTransaction;
   }

   private void registerRemoteTransaction(GlobalTransaction gtx, RemoteTransaction rtx) {
      RemoteTransaction transaction = remoteTransactions.put(gtx, rtx);
      if (transaction != null) {
         log.remoteTxAlreadyRegistered();
         throw new IllegalStateException("A remote transaction with the given id was already registered!!!");
      }

      log.trace("Created and registered remote transaction " + rtx);
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
         current = txFactory.newLocalTransaction(transaction, tx, ctx.isImplicitTransaction());
         updateViewId(current);
         log.tracef("Created a new tx: %s", current);
         localTransactions.put(transaction, current);
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

   public LocalTransaction removeLocalTransaction(Transaction tx) {
      return removeLocalTransactionInternal(tx);
   }

   protected final LocalTransaction removeLocalTransactionInternal(Transaction tx) {
      LocalTransaction removed;
      synchronized (minViewIdInvariant) {
         removed = localTransactions.remove(tx);
         if (removed != null) {
            recalculateMinViewIdIfNeeded(removed);
            removed.notifyOnTransactionFinished();
         }
      }
      return removed;
   }

   /**
    * Removes the {@link RemoteTransaction} corresponding to the given tx.
    */
   public void remoteTransactionCommitted(GlobalTransaction gtx) {
      if (configuration.isSecondPhaseAsync()) {
         removeRemoteTransaction(gtx);
      }
   }

   public final RemoteTransaction removeRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction result;
      synchronized (minViewIdInvariant) {
         result = remoteTransactions.remove(txId);
         if (result == null) return null;
         recalculateMinViewIdIfNeeded(result);
      }
      log.tracef("Removed %s from transaction table. Transaction %s", txId, result);
      result.notifyOnTransactionFinished();
      return result;
   }

   public int getRemoteTxCount() {
      return remoteTransactions.size();
   }

   public int getLocalTxCount() {
      return localTransactions.size();
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

   protected final LocalTransaction getLocalTx(Transaction tx) {
      return localTransactions.get(tx);
   }

   private boolean isStandaloneCache() {
      return rpcManager == null;
   }

   private int getCurrentViewId() {
      return rpcManager.getTransport().getViewId();
   }

   public final Collection<LocalTransaction> getLocalTransactions() {
      return localTransactions.values();
   }

   protected final void recalculateMinViewIdIfNeeded(CacheTransaction removeTx) {
      if (isStandaloneCache()) return;
      if (removeTx == null)
         throw new NullPointerException("Cannot pass a null tx here!");
      if (removeTx.getViewId() < minTxViewId)
         throw new IllegalStateException("Cannot have a tx that has a viewId(" + removeTx.getViewId()
                                               + ") smaller than min view id (" + minTxViewId + ")");

      if (removeTx.getViewId() != getCurrentViewId()) {
         calculateMinViewId();
      }
   }

   @ViewChanged
   public void recalculateMinViewIdOnTopologyChange(ViewChangedEvent vce) {
      // don't do anything if this cache is not clustered - view changes are global
      if (rpcManager == null)
         return;

      log.debugf("View changed, recalculating minViewId");
      synchronized (minViewIdInvariant) {
         calculateMinViewId();
      }
   }


   private void calculateMinViewId() {
      int minViewId = getCurrentViewId();

      for (CacheTransaction ct : localTransactions.values()) {
         if (ct.getViewId() < minViewId) {
            minViewId = ct.getViewId();
            log.tracef("Setting minViewId to %s form local transaction %s", ct.getViewId(), ct);
         }
      }
      for (CacheTransaction ct : remoteTransactions.values()) {
         if (ct.getViewId() < minViewId) {
            minViewId = ct.getViewId();
            log.tracef("Setting minViewId to %s form remote transaction %s", ct.getViewId(), ct);
         }
      }
      log.tracef("Recalculating min view id: existingValue=%s, minViewId=%s", minTxViewId, minViewId);
      minTxViewId = minViewId;
   }

   private void updateViewId(CacheTransaction current) {
      if (!isStandaloneCache()) {
         current.setViewId(getCurrentViewId());
      }
   }

   private boolean areTxsOnGoing() {
      return !localTransactions.isEmpty() || !remoteTransactions.isEmpty();
   }

   private void shutDownGracefully() {
      log.debugf("Wait for on-going transactions to finish for %d seconds.", TimeUnit.MILLISECONDS.toSeconds(configuration.getCacheStopTimeout()));
      long failTime = System.currentTimeMillis() + configuration.getCacheStopTimeout();
      boolean txsOnGoing = areTxsOnGoing();
      while (txsOnGoing && System.currentTimeMillis() < failTime) {
         try {
            Thread.sleep(100);
            txsOnGoing = areTxsOnGoing();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debugf("Interrupted waiting for on-going transactions to finish. localTransactions=%s, remoteTransactions%s",
                       localTransactions, remoteTransactions);
         }
      }

      if (txsOnGoing) {
         log.unfinishedTransactionsRemain(localTransactions, remoteTransactions);
      } else {
         log.trace("All transactions terminated");
      }
   }
}
