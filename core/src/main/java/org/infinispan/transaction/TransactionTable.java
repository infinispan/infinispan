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
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.synchronization.SynchronizationAdapter;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.concurrent.locks.LockManager;
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
   private TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   private LockManager lockManager;

   @Inject
   public void initialize(RpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker, CacheNotifier notifier,
                          TransactionFactory gtf, EmbeddedCacheManager cm, TransactionCoordinator txCoordinator,
                          TransactionSynchronizationRegistry transactionSynchronizationRegistry, LockManager lockManager) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
      this.txFactory = gtf;
      this.cm = cm;
      this.txCoordinator = txCoordinator;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.lockManager = lockManager;
   }

   @Start
   private void start() {
      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, "LockBreakingService," + configuration.getName()
                  + "," + rpcManager.getTransport().getAddress());
            th.setDaemon(true);
            return th;
         }
      };
      lockBreakingService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(), tf,
                                              new ThreadPoolExecutor.CallerRunsPolicy());
      cm.addListener(listener);
      notifier.addListener(listener);
   }

   @Stop
   private void stop() {
      notifier.removeListener(listener);
      cm.removeListener(listener);
      lockBreakingService.shutdownNow();
      if (trace) log.tracef("Wait for on-going transactions to finish for %d seconds.", TimeUnit.MILLISECONDS.toSeconds(configuration.getCacheStopTimeout()));
      long failTime = System.currentTimeMillis() + configuration.getCacheStopTimeout();
      boolean txsOnGoing = areTxsOnGoing();
      while (txsOnGoing && System.currentTimeMillis() < failTime) {
         try {
            Thread.sleep(100);
            txsOnGoing = areTxsOnGoing();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (trace) {
               log.tracef("Interrupted waiting for on-going transactions to finish. localTransactions=%s, remoteTransactions%s",
                     localTransactions, remoteTransactions);
            }
         }
      }

      if (txsOnGoing) {
         log.unfinishedTransactionsRemain(localTransactions, remoteTransactions);
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

   @Listener
   public class StaleTransactionCleanup {
      /**
       * Roll back remote transactions originating on nodes that have left the cluster.
       */
      @ViewChanged
      public void onViewChange(ViewChangedEvent vce) {
         final List<Address> leavers = MembershipArithmetic.getMembersLeft(vce.getOldMembers(),
                                                                           vce.getNewMembers());
         if (!leavers.isEmpty()) {
            if (trace) log.tracef("Saw %d leavers - kicking off a lock breaking task", leavers.size());
            cleanTxForWhichTheOwnerLeft(leavers);
         }
      }

      /**
       * Roll back local transactions that have acquired lock that are no longer valid,
       * either because the main data owner left the cluster or because a node joined
       * the cluster and is the new data owner.
       * This method will only ever be called in distributed mode.
       */
      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent tce) {
         // do all the work AFTER the consistent hash has changed
         if (tce.isPre())
            return;

         Address self = rpcManager.getAddress();
         ConsistentHash chOld = tce.getConsistentHashAtStart();
         ConsistentHash chNew = tce.getConsistentHashAtEnd();

         if (configuration.isEagerLockingSingleNodeInUse()) {
            log.tracef("Cleaning local transactions with stale eager single node locks");
            // roll back local transactions if their main data owner has changed
            for (LocalTransaction localTx : localTransactions.values()) {
               for (Object key : localTx.getAffectedKeys()) {
                  List<Address> oldPrimaryOwner = chOld.locate(key, 1);
                  List<Address> newPrimaryOwner = chNew.locate(key, 1);
                  if (!oldPrimaryOwner.get(0).equals(newPrimaryOwner.get(0))) {
                     localTx.markForRollback(true);
                     log.tracef("Marked local transaction %sfor rollback, as the main data " +
                           "owner has changed from %s to %s", localTx.getGlobalTransaction(),
                           oldPrimaryOwner, newPrimaryOwner);
                     break;
                  }
               }
            }
            log.tracef("Finished cleaning local transactions with stale eager single node locks");
         }

         // for remote transactions, release locks for which we are no longer an owner
         // only for remote transactions, since we acquire locks on the origin node regardless if it's the owner or not
         log.tracef("Unlocking keys for which we are no longer an owner");
         int numOwners = configuration.isEagerLockingSingleNodeInUse() ? 1 : configuration.getNumOwners();
         for (RemoteTransaction remoteTx : remoteTransactions.values()) {
            GlobalTransaction gtx = remoteTx.getGlobalTransaction();
            List<Object> keys = new ArrayList<Object>();
            boolean txHasLocalKeys = false;
            for (Object key : remoteTx.getLockedKeys()) {
               boolean wasLocal = chOld.isKeyLocalToAddress(self, key, numOwners);
               boolean isLocal = chNew.isKeyLocalToAddress(self, key, numOwners);
               if (wasLocal && !isLocal) {
                  keys.add(key);
               }
               txHasLocalKeys |= isLocal;
            }
            if (keys.size() > 0) {
               if (trace) log.tracef("Unlocking keys %s for remote transaction %s as we are no longer an owner", keys, gtx);
               Set<Flag> flags = EnumSet.of(Flag.CACHE_MODE_LOCAL);
               String cacheName = configuration.getName();
               LockControlCommand unlockCmd = new LockControlCommand(keys, cacheName, flags, false);
               unlockCmd.init(invoker, icc, TransactionTable.this);
               unlockCmd.attachGlobalTransaction(gtx);
               unlockCmd.setUnlock(true);
               try {
                  unlockCmd.perform(null);
                  log.tracef("Unlocking moved keys for %s complete.", gtx);
               } catch (Throwable t) {
                  log.unableToUnlockRebalancedKeys(gtx, keys, self, t);
               }

               // if the transaction doesn't touch local keys any more, we can roll it back
               if (!txHasLocalKeys) {
                  if (trace) log.tracef("Killing remote transaction without any local keys %s", gtx);
                  RollbackCommand rc = new RollbackCommand(cacheName, gtx);
                  rc.init(invoker, icc, TransactionTable.this);
                  try {
                     rc.perform(null);
                     log.tracef("Rollback of transaction %s complete.", gtx);
                  } catch (Throwable e) {
                     log.unableToRollbackGlobalTx(gtx, e);
                  } finally {
                     removeRemoteTransaction(gtx);
                  }
               }
            }
         }

         log.trace("Finished cleaning locks for keys that are no longer local");
      }

      private void cleanTxForWhichTheOwnerLeft(final Collection<Address> leavers) {
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

   protected void updateStateOnNodesLeaving(Collection<Address> leavers) {
      Set<GlobalTransaction> toKill = new HashSet<GlobalTransaction>();
      for (GlobalTransaction gt : remoteTransactions.keySet()) {
         if (leavers.contains(gt.getAddress())) toKill.add(gt);
      }

      if (trace) {
         if (toKill.isEmpty())
            log.tracef("No global transactions pertain to originator(s) %s who have left the cluster.", leavers);
         else
            log.tracef("%s global transactions pertain to leavers list %s and need to be killed", toKill.size(), leavers);
      }

      for (GlobalTransaction gtx : toKill) {
         if (trace) log.tracef("Killing remote transaction originating on leaver %s", gtx);
         RollbackCommand rc = new RollbackCommand(configuration.getName(), gtx);
         rc.init(invoker, icc, TransactionTable.this);
         try {
            rc.perform(null);
            if (trace) log.tracef("Rollback of transaction %s complete.", gtx);
         } catch (Throwable e) {
            log.unableToRollbackGlobalTx(gtx, e);
         } finally {
            removeRemoteTransaction(gtx);
         }
      }

      if (trace) log.trace("Completed cleaning transactions originating on leavers");
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
         log.remoteTxAlreadyRegistered();
         throw new IllegalStateException("A remote transaction with the given id was already registered!!!");
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
         if (trace) log.tracef("Created a new GlobalTransaction %s", tx);
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
      return localTransaction != null &&
            localTransactions.remove(localTransaction.getTransaction()) != null;
   }

   /**
    * Removes the {@link RemoteTransaction} corresponding to the given tx.
    */
   public void remoteTransactionCompleted(GlobalTransaction gtx, boolean committed) {
      RemoteTransaction remove = remoteTransactions.remove(gtx);
      if (log.isTraceEnabled()) log.tracef("Removing remote transaction as it is completed: %s", remove);
   }

   private boolean removeRemoteTransaction(GlobalTransaction txId) {
      boolean existed = remoteTransactions.remove(txId) != null;
      if (trace) {
         log.tracef("Removed %s from transaction table. Transaction existed? %b", txId, existed);
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
