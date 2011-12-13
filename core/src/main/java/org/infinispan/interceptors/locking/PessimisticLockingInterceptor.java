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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Locking interceptor to be used by pessimistic caches.
 * Design note: when a lock "k" needs to be acquired (e.g. cache.put("k", "v")), if the lock owner is the local node,
 * no remote call is performed to migrate locking logic to the other (numOwners - 1) lock owners. This is a good
 * optimisation for  in-vm transactions: if the local node crashes before prepare then the replicated lock information
 * would be useless as the tx is rolled back. OTOH for remote hotrod/transactions this additional RPC makes sense because
 * there's no such thing as transaction originator node, so this might become a configuration option when HotRod tx are
 * in place.
 *
 * Implementation note: current implementation acquires locks remotely first and then locally. This is required
 * by the deadlock detection logic, but might not be optimal: acquiring locks locally first might help to fail fast the
 * in the case of keys being locked.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class PessimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   private CommandsFactory cf;
   private RpcManager rpcManager;

   private static final Log log = LogFactory.getLog(OptimisticLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory factory, RpcManager rpcManager) {
      this.rpcManager = rpcManager;
      this.cf = factory;
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         if (ctx.hasFlag(Flag.FORCE_WRITE_LOCK)) {
            lockKeyAndCheckOwnership(ctx, command.getKey());
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable t) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw t;
      } finally {
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         abortIfRemoteTransactionInvalid(ctx, command);
         return invokeNextAndCommitIf1Pc(ctx, command);
      } catch (Throwable t) {
         // don't remove the locks here, the rollback command will clear them
         throw t;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      // needed by the stat transfer.
      if (ctx.hasFlag(Flag.SKIP_LOCKING))
         return invokeNextInterceptor(ctx, command);

      final TxInvocationContext txContext = (TxInvocationContext) ctx;
      try {
         Object result = invokeNextInterceptor(ctx, command);
         final boolean localNodeOwnsLock = cdl.localNodeIsPrimaryOwner(command.getKey());
         acquireRemoteIfNeeded(ctx, command.getKey(), localNodeOwnsLock);
         if (ctx.hasFlag(Flag.SKIP_OWNERSHIP_CHECK) || localNodeOwnsLock) {
            lockKeyAndCheckOwnership(ctx, command.getKey());
         } else if (cdl.localNodeIsOwner(command.getKey())) {
            txContext.getCacheTransaction().addBackupLockForKey(command.getKey());
         }
         return result;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         final Object result = invokeNextInterceptor(ctx, command);
         acquireRemoteIfNeeded(ctx, command.getMap().keySet());
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         for (Object key : command.getMap().keySet()) {
            lockAndRegisterBackupLock(txContext, key);
         }
         return result;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         final Object result = invokeNextInterceptor(ctx, command);
         final boolean localNodeOwnsLock = cdl.localNodeIsPrimaryOwner(command.getKey());
         acquireRemoteIfNeeded(ctx, command.getKey(), localNodeOwnsLock);
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txContext, command.getKey(), localNodeOwnsLock);
         return result;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }
   
   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      Object[] compositeKeys = command.getCompositeKeys();
      try {
         HashSet<Object> keysToLock = new HashSet<Object>(Arrays.asList(compositeKeys));
         acquireRemoteIfNeeded(ctx, keysToLock);
         if (cdl.localNodeIsOwner(command.getDeltaAwareKey())) {
            for (Object key : compositeKeys) {
               lockKey(ctx, key);   
            }      
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         final Object result = invokeNextInterceptor(ctx, command);
         final boolean localNodeOwnsLock = cdl.localNodeIsPrimaryOwner(command.getKey());
         acquireRemoteIfNeeded(ctx, command.getKey(), localNodeOwnsLock);
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         lockAndRegisterBackupLock(txContext, command.getKey(), localNodeOwnsLock);
         return result;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         for (InternalCacheEntry entry : dataContainer.entrySet())
            lockAndRegisterBackupLock((TxInvocationContext) ctx, entry.getKey());
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (!ctx.isInTxScope())
         throw new IllegalStateException("Locks should only be acquired within the scope of a transaction!");

      //first go remotely - required by DLD. Only acquire remote lock if multiple keys or the single key doesn't map
      // to the local node.
      if (ctx.isOriginLocal()) {
         final boolean isSingleKeyAndLocal = !command.multipleKeys() && cdl.localNodeIsPrimaryOwner(command.getSingleKey());
         if (!isSingleKeyAndLocal || command.multipleKeys()) {
            LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
            if (!localTx.getAffectedKeys().containsAll(command.getKeys())) {
               invokeNextInterceptor(ctx, command);
               ctx.addAllAffectedKeys(command.getKeys());
            } else {
               log.tracef("Already own locks on keys: %s, skipping remote call", command.getKeys());
            }
         }
      }

      try {
         abortIfRemoteTransactionInvalid(ctx, command);
         if (command.isUnlock()) {
            if (ctx.isOriginLocal())
               throw new AssertionError("There's no advancedCache.unlock so this must have originated remotely.");
            releaseLocksOnFailureBeforePrepare(ctx);
            return false;
         }

         for (Object key : command.getKeys()) {
            lockAndRegisterBackupLock(ctx, key);
         }
         return true;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   private void acquireRemoteIfNeeded(InvocationContext ctx, Set<Object> keys) throws Throwable {
      if (ctx.isOriginLocal() && !ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         if (localTransaction.getAffectedKeys().containsAll(keys)) {
            log.tracef("We already have lock for keys %s, skip remote lock acquisition", keys);
            return;
         } else {
            LockControlCommand lcc = cf.buildLockControlCommand(keys, ctx.getFlags(), txContext.getGlobalTransaction());
            invokeNextInterceptor(ctx, lcc);
         }
      }
      ((TxInvocationContext) ctx).addAllAffectedKeys(keys);
   }

   private void acquireRemoteIfNeeded(InvocationContext ctx, Object key, boolean localNodeIsLockOwner) throws Throwable {
      if (!localNodeIsLockOwner && ctx.isOriginLocal() && !ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         final boolean alreadyLocked = localTransaction.getAffectedKeys().contains(key);
         if (alreadyLocked) {
            log.tracef("We already have lock for key %s, skip remote lock acquisition", key);
            return;
         } else {
            LockControlCommand lcc = cf.buildLockControlCommand(key, ctx.getFlags(), txContext.getGlobalTransaction());
            invokeNextInterceptor(ctx, lcc);
         }
      }
      ((TxInvocationContext) ctx).addAffectedKey(key);
   }

   private void releaseLocksOnFailureBeforePrepare(InvocationContext ctx) {
      lockManager.unlockAll(ctx);
      if (ctx.isOriginLocal() && rpcManager != null) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         TxCompletionNotificationCommand command = cf.buildTxCompletionNotificationCommand(null, txContext.getGlobalTransaction());
         final LocalTransaction cacheTransaction = (LocalTransaction) txContext.getCacheTransaction();
         rpcManager.invokeRemotely(cacheTransaction.getRemoteLocksAcquired(), command, true);
      }
   }

   protected final void lockAndRegisterBackupLock(TxInvocationContext ctx, Object key, boolean isLockOwner) throws InterruptedException {
      if (isLockOwner) {
         lockKeyAndCheckOwnership(ctx, key);
      } else if (cdl.localNodeIsOwner(key)) {
         ctx.getCacheTransaction().addBackupLockForKey(key);
      }
   }
}
