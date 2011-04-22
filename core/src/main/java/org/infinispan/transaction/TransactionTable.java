/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.synchronization.SynchronizationAdapter;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;

/**
 * Repository for {@link RemoteTransaction} and {@link
 * org.infinispan.transaction.xa.TransactionXaAdapter}s (locally originated transactions).
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class TransactionTable {

   private static final Log log = LogFactory.getLog(TransactionTable.class);
   private static boolean trace = log.isTraceEnabled();

   protected final ConcurrentMap<Transaction, LocalTransaction> localTransactions = new ConcurrentHashMap<Transaction, LocalTransaction>();

   protected final ConcurrentMap<GlobalTransaction, RemoteTransaction> remoteTransactions = new ConcurrentHashMap<GlobalTransaction, RemoteTransaction>();


   private final Object listener = new StaleTransactionCleanup();
   
   protected Configuration configuration;
   protected InvocationContextContainer icc;
   protected TransactionCoordinator txCoordinator;
   protected TransactionFactory txFactory;
   private InterceptorChain invoker;
   private CacheNotifier notifier;
   private RpcManager rpcManager;
   private ExecutorService lockBreakingService;
   private EmbeddedCacheManager cm;

   @Inject
   public void initialize(RpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker, CacheNotifier notifier,
                          TransactionFactory gtf, EmbeddedCacheManager cm, TransactionCoordinator txCoordinator) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
      this.txFactory = gtf;
      this.cm = cm;
      this.txCoordinator = txCoordinator;
   }

   @Start
   private void start() {
      lockBreakingService = Executors.newFixedThreadPool(1);
      cm.addListener(listener);
   }

   @Stop
   private void stop() {
      cm.removeListener(listener);
      lockBreakingService.shutdownNow();
      if (trace) log.trace("Wait for on-going transactions to finish for %d seconds.", TimeUnit.MILLISECONDS.toSeconds(configuration.getCacheStopTimeout()));
      long failTime = System.currentTimeMillis() + configuration.getCacheStopTimeout();
      boolean txsOnGoing = areTxsOnGoing();
      while (txsOnGoing && System.currentTimeMillis() < failTime) {
         try {
            Thread.sleep(100);
            txsOnGoing = areTxsOnGoing();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (trace) {
               log.trace("Interrupted waiting for on-going transactions to finish. localTransactions=%s, remoteTransactions%s",
                     localTransactions, remoteTransactions);
            }
         }
      }

      if (txsOnGoing) {
         log.warn("Stopping but there're transactions that did not finish in time: localTransactions=%s, remoteTransactions%s",
                  localTransactions, remoteTransactions);
      } else {
         if (trace) log.trace("All transactions terminated");
      }
   }

   private boolean areTxsOnGoing() {
      return !localTransactions.isEmpty() || !remoteTransactions.isEmpty();
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
         SynchronizationAdapter sync = new SynchronizationAdapter(localTransaction, txCoordinator);
         try {
            transaction.registerSynchronization(sync);
         } catch (Exception e) {
            log.warn("Failed synchronization registration", e);
            throw new CacheException(e);
         }
         ((SyncLocalTransaction) localTransaction).setEnlisted(true);
      }
   }

   public void failureCompletingTransaction(Transaction tx) {
      removeLocalTransaction(localTransactions.get(tx));
   }

   @Listener
   public class StaleTransactionCleanup {
      @ViewChanged
      public void onViewChange(ViewChangedEvent vce) {
         final List<Address> leavers = MembershipArithmetic.getMembersLeft(vce.getOldMembers(), vce.getNewMembers());
         if (!leavers.isEmpty()) {
            if (trace) log.trace("Saw %s leavers - kicking off a lock breaking task", leavers.size());
            cleanTxForWhichTheOwnerLeft(leavers);
            if (configuration.isUseEagerLocking() && configuration.isEagerLockSingleNode() && configuration.getCacheMode().isDistributed()) {
               for (LocalTransaction localTx : localTransactions.values()) {
                  if (localTx.hasRemoteLocksAcquired(leavers)) {
                     localTx.markForRollback(true);
                  }
               }
            }
         }
      }

      private void cleanTxForWhichTheOwnerLeft(final List<Address> leavers) {
         try {
            lockBreakingService.submit(new Runnable() {
               public void run() {
                  try {
                  updateStateOnNodesLeaving(leavers);
                  } catch (Exception e) {
                     log.error("Exception whilst updating state", e);
                  }
               }
            });
         } catch (RejectedExecutionException ree) {
            log.debug("Unable to submit task to executor", ree);
         }

      }
   }

   protected void updateStateOnNodesLeaving(List<Address> leavers) {
      Set<GlobalTransaction> toKill = new HashSet<GlobalTransaction>();
      for (GlobalTransaction gt : remoteTransactions.keySet()) {
         if (leavers.contains(gt.getAddress())) toKill.add(gt);
      }

      if (trace) {
         if (toKill.isEmpty())
            log.trace("No global transactions pertain to originator(s) %s who have left the cluster.", leavers);
         else
            log.trace("%s global transactions pertain to leavers list %s and need to be killed", toKill.size(), leavers);
      }

      for (GlobalTransaction gtx : toKill) {
         if (trace) log.trace("Killing %s", gtx);
         RollbackCommand rc = new RollbackCommand(gtx);
         rc.init(invoker, icc, TransactionTable.this);
         try {
            rc.perform(null);
            if (trace) log.trace("Rollback of %s complete.", gtx);
         } catch (Throwable e) {
            log.warn("Unable to roll back gtx " + gtx, e);
         } finally {
            removeRemoteTransaction(gtx);
         }
      }

      if (trace) log.trace("Completed cleaning stale locks.");
   }

   /**
    * Returns the {@link RemoteTransaction} associated with the supplied transaction id.
    * Returns null if no such association exists.
    */
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      return remoteTransactions.get(txId);
   }

   /**
    * Creates and register a {@link RemoteTransaction} based on the supplied params.
    * Returns the created transaction.
    *
    * @throws IllegalStateException if an attempt to create a {@link RemoteTransaction}
    *                               for an already registered id is made.
    */
   public RemoteTransaction createRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications) {
      RemoteTransaction remoteTransaction = txFactory.newRemoteTransaction(modifications, globalTx);
      registerRemoteTransaction(globalTx, remoteTransaction);
      return remoteTransaction;
   }

   /**
    * Creates and register a {@link RemoteTransaction} with no modifications. Returns the
    * created transaction.
    *
    * @throws IllegalStateException if an attempt to create a {@link RemoteTransaction}
    *                               for an already registered id is made.
    */
   public RemoteTransaction createRemoteTransaction(GlobalTransaction globalTx) {
      RemoteTransaction remoteTransaction = txFactory.newRemoteTransaction(globalTx);
      registerRemoteTransaction(globalTx, remoteTransaction);
      return remoteTransaction;
   }

   private void registerRemoteTransaction(GlobalTransaction gtx, RemoteTransaction rtx) {
      RemoteTransaction transaction = remoteTransactions.put(gtx, rtx);
      if (transaction != null) {
         String message = "A remote transaction with the given id was already registered!!!";
         log.error(message);
         throw new IllegalStateException(message);
      }

      if (trace) log.trace("Created and registered remote transaction " + rtx);
   }

   /**
    * Returns the {@link org.infinispan.transaction.xa.TransactionXaAdapter} corresponding to the supplied transaction.
    * If none exists, will be created first.
    */
   public LocalTransaction getOrCreateLocalTransaction(Transaction transaction, InvocationContext ctx) {
      LocalTransaction current = localTransactions.get(transaction);
      if (current == null) {
         Address localAddress = rpcManager != null ? rpcManager.getTransport().getAddress() : null;
         GlobalTransaction tx = txFactory.newGlobalTransaction(localAddress, false);
         if (trace) log.trace("Created a new GlobalTransaction %s", tx);
         current = txFactory.newLocalTransaction(transaction, tx);
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
      return localTransactions.remove(localTransaction.getTransaction()) != null;
   }

   /**
    * Removes the {@link RemoteTransaction} corresponding to the given tx.
    */
   public void remoteTransactionCompleted(GlobalTransaction gtx, boolean committed) {
      RemoteTransaction remove = remoteTransactions.remove(gtx);
      if (log.isTraceEnabled()) log.trace("Removing remote transaction as it is completed: %s", remove);
   }

   private boolean removeRemoteTransaction(GlobalTransaction txId) {
      boolean existed = remoteTransactions.remove(txId) != null;
      if (trace) {
         log.trace("Removed " + txId + " from transaction table. Transaction existed? " + existed);
      }
      return existed;
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
}
