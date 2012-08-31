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

package org.infinispan.interceptors.locking;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * Base class for transaction based locking interceptors.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.1
 */
public abstract class AbstractTxLockingInterceptor extends AbstractLockingInterceptor {

   protected TransactionTable txTable;
   protected RpcManager rpcManager;
   private boolean clustered;

   @Inject
   public void setDependencies(TransactionTable txTable, RpcManager rpcManager) {
      this.txTable = txTable;
      this.rpcManager = rpcManager;
   }

   @Start
   private void setClustered() {
      clustered = rpcManager != null;
   }


   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public final Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // ensure keys are properly locked for evict commands
      ctx.setFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT, Flag.CACHE_MODE_LOCAL);
      try {
         lockKey(ctx, command.getKey());
         return invokeNextInterceptor(ctx, command);
      } finally {
         //evict doesn't get called within a tx scope, so we should apply the changes before returning
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         return super.visitGetKeyValueCommand(ctx, command);
      } finally {
         //when not invoked in an explicit tx's scope the get is non-transactional(mainly for efficiency).
         //locks need to be released in this situation as they might have been acquired from L1.
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
      return super.visitCommitCommand(ctx, command);
      } finally {
         if (releaseLockOnTxCompletion(ctx)) lockManager.unlockAll(ctx);
      }
   }

   protected final void abortIfRemoteTransactionInvalid(TxInvocationContext ctx, AbstractTransactionBoundaryCommand c) {
      // this check fixes ISPN-777
      if (!ctx.isOriginLocal()) {
         Address origin = c.getGlobalTransaction().getAddress();
         if (!rpcManager.getTransport().getMembers().contains(origin))
            throw new CacheException("Member " + origin + " no longer in cluster. Forcing tx rollback for " + c.getGlobalTransaction());
      }
   }

   protected final Object invokeNextAndCommitIf1Pc(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit() && releaseLockOnTxCompletion(ctx)) {
         lockManager.unlockAll(ctx);
      }
      return result;
   }

   /**
    * The backup (non-primary) owners keep a "backup lock" for each key they received in a lock/prepare command.
    * Normally there can be many transactions holding the backup lock at the same time, but when the secondary owner
    * becomes a primary owner a new transaction trying to obtain the "real" lock will have to wait for all backup
    * locks to be released. The backup lock will be released either by a commit/rollback/unlock command or by
    * the originator leaving the cluster (if recovery is disabled).
    */
   protected final void lockAndRegisterBackupLock(TxInvocationContext ctx, Object key) throws InterruptedException {
      if (cdl.localNodeIsPrimaryOwner(key)) {
         lockKeyAndCheckOwnership(ctx, key);
      } else if (cdl.localNodeIsOwner(key)) {
         ctx.getCacheTransaction().addBackupLockForKey(key);
      }
   }

   /**
    * Besides acquiring a lock, this method also handles the following situation:
    * 1. consistentHash("k") == {A, B}, tx1 prepared on A and B. Then node A crashed (A  == single lock owner)
    * 2. at this point tx2 which also writes "k" tries to prepare on B.
    * 3. tx2 has to determine that "k" is already locked by another tx (i.e. tx1) and it has to wait for that tx to finish before acquiring the lock.
    *
    * The algorithm used at step 3 is:
    * - the transaction table(TT) associates the current view id with every remote and local transaction it creates
    * - TT also keeps track of the minimal value of all the view ids of all the transactions still present in the cache (minViewId)
    * - when a tx wants to acquire lock "k":
    *    - if tx.viewId > TT.minViewId then "k" might be a key whose owner crashed. If so:
    *       - obtain the list LT of transactions that started in a previous view (txTable.getTransactionsPreparedBefore)
    *       - for each t in LT:
    *          - if t wants to write "k" then block until t finishes (CacheTransaction.waitForTransactionsToFinishIfItWritesToKey)
    *       - only then try to acquire lock on "k"
    *    - if tx.viewId == TT.minViewId try to acquire lock straight away.
    *
    * Note: The algorithm described below only when nodes leave the cluster, so it doesn't add a performance burden
    * when the cluster is stable.
    */
   protected final void lockKeyAndCheckOwnership(InvocationContext ctx, Object key) throws InterruptedException {
      boolean checkForPendingLocks = false;
      //this is possible when the put is originated as a result of a state transfer
      if (!ctx.isInTxScope()) {
         lockManager.acquireLock(ctx, key);
         return;
      }
      TxInvocationContext txContext = (TxInvocationContext) ctx;
      int transactionViewId = -1;
      if (clustered) {
         transactionViewId = txContext.getCacheTransaction().getViewId();
         if (transactionViewId != TransactionTable.CACHE_STOPPED_VIEW_ID) {
            checkForPendingLocks = transactionViewId > txTable.getMinViewId();
         }
      }

      if (checkForPendingLocks) {
         getLog().tracef("Checking for pending locks and then locking key %s", key);

         long expectedEndTime = nowMillis() + cacheConfiguration.locking().lockAcquisitionTimeout();

         // Check local transactions first
         for (CacheTransaction ct: txTable.getLocalTransactions()) {
            if (ct.getViewId() < transactionViewId) {
               long remaining = expectedEndTime - nowMillis();
               if (remaining < 0 || !ct.waitForLockRelease(key, remaining)) throw newTimeoutException(key, txContext);
            }
         }

         // ... then remote ones
         for (CacheTransaction ct: txTable.getRemoteTransactions()) {
            if (ct.getViewId() < transactionViewId) {
               long remaining = expectedEndTime - nowMillis();
               if (remaining < 0 || !ct.waitForLockRelease(key, remaining)) throw newTimeoutException(key, txContext);
            }
         }

         // Then try to acquire a lock
         final long remaining = expectedEndTime - nowMillis();
         if (remaining <= 0) {
            throw newTimeoutException(key, txContext);
         } else {
            getLog().tracef("Finished waiting for other potential lockers, trying to acquire the lock on %s", key);
            lockManager.acquireLock(ctx, key, remaining);
         }
      } else {
         getLog().tracef("Locking key %s, no need to check for pending locks.", key);
         lockManager.acquireLock(ctx, key);
      }
   }

   private TimeoutException newTimeoutException(Object key, TxInvocationContext txContext) {
      return new TimeoutException("Could not acquire lock on " + key + " on behalf of transaction " +
                                       txContext.getGlobalTransaction() + ". Lock is being held by " + lockManager.getOwner(key));
   }

   private boolean releaseLockOnTxCompletion(TxInvocationContext ctx) {
      return ctx.isOriginLocal() || Configurations.isSecondPhaseAsync(cacheConfiguration);
   }

   private long nowMillis() {
      //use nanos as is more precise and less expensive.
      return System.nanoTime() / 1000000;
   }
}
