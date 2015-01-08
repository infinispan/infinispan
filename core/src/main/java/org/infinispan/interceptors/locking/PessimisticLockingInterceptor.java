package org.infinispan.interceptors.locking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private StateTransferManager stateTransferManager;

   private static final Log log = LogFactory.getLog(PessimisticLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory factory, StateTransferManager stateTransferManager) {
      this.cf = factory;
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   protected final Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      try {
         if (ctx.isInTxScope() && command.hasFlag(Flag.FORCE_WRITE_LOCK) && !hasSkipLocking(command)) {
            acquireRemoteIfNeeded(ctx, command, cdl.localNodeIsPrimaryOwner(command.getKey()));
            long lockTimeout = getLockAcquisitionTimeout(command, false);
            lockKeyAndCheckOwnership(ctx, command.getKey(), lockTimeout, false);
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
      return invokeNextAndCommitIf1Pc(ctx, command);
      // don't remove the locks here, the rollback command will clear them
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         boolean skipLocking = hasSkipLocking(command);
         if (!skipLocking) {
            acquireRemoteIfNeeded(ctx, command.getMap().keySet(), command);
            final TxInvocationContext txContext = (TxInvocationContext) ctx;
            long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
            for (Object key : command.getMap().keySet()) {
               lockAndRegisterBackupLock(txContext, key, lockTimeout, skipLocking);
            }
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      try {
         boolean skipLocking = hasSkipLocking(command);
         if (!skipLocking) {
            final boolean localLock = cdl.localNodeIsPrimaryOwner(command.getKey());
            acquireRemoteIfNeeded(ctx, command, localLock);
            final TxInvocationContext txContext = (TxInvocationContext) ctx;
            long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
            lockAndRegisterBackupLock(txContext, command.getKey(), localLock, lockTimeout, skipLocking);
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      Object[] compositeKeys = command.getCompositeKeys();
      try {
         boolean skipLocking = hasSkipLocking(command);
         if (!skipLocking) {
            HashSet<Object> keysToLock = new HashSet<Object>(Arrays.asList(compositeKeys));
            acquireRemoteIfNeeded(ctx, keysToLock, command);
            if (cdl.localNodeIsOwner(command.getKey())) {
               long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
               for (Object key : compositeKeys) {
                  lockKey(ctx, key, lockTimeout, skipLocking);
               }
            }
         }
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      try {
         boolean skipLocking = hasSkipLocking(command);
         if (!skipLocking) {
            // TODO The clear command doesn't acquire any remote locks. See ISPN-4140
            long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
            for (InternalCacheEntry entry : dataContainer.entrySet())
               lockAndRegisterBackupLock((TxInvocationContext) ctx,
                     entry.getKey(), lockTimeout, skipLocking);
         }

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

      try {
         boolean skipLocking = hasSkipLocking(command);
         if (skipLocking) {
            return invokeNextInterceptor(ctx, command);
         }

         // First go remotely - required by DLD.
         // Only acquire remote lock if multiple keys or the single key primary owner doesn't map to the local node.
         if (ctx.isOriginLocal()) {
            final boolean isSingleKeyAndLocal = !command.multipleKeys() && cdl.localNodeIsPrimaryOwner(command.getSingleKey());
            boolean needBackupLocks = !isSingleKeyAndLocal || isStateTransferInProgress();
            if (needBackupLocks && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
               LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
               if (!localTx.getAffectedKeys().containsAll(command.getKeys())) {
                  invokeNextInterceptor(ctx, command);
               } else {
                  log.tracef("Already own locks on keys: %s, skipping remote call", command.getKeys());
               }
            }
            ctx.addAllAffectedKeys(command.getKeys());
         }

         if (command.isUnlock()) {
            if (ctx.isOriginLocal())
               throw new AssertionError("There's no advancedCache.unlock so this must have originated remotely.");
            releaseLocksOnFailureBeforePrepare(ctx);
            return Boolean.FALSE;
         }

         long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
         for (Object key : command.getKeys()) {
            lockAndRegisterBackupLock(ctx, key, lockTimeout, skipLocking);
         }
         return Boolean.TRUE;
      } catch (Throwable te) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw te;
      }
   }

   private void acquireRemoteIfNeeded(InvocationContext ctx, Set<Object> keys, FlagAffectedCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         if (localTransaction.getAffectedKeys().containsAll(keys)) {
            log.tracef("We already have lock for keys %s, skip remote lock acquisition", keys);
            return;
         } else {
            LockControlCommand lcc = cf.buildLockControlCommand(keys,
                  command.getFlags(), txContext.getGlobalTransaction());
            invokeNextInterceptor(ctx, lcc);
         }
      }
      ((TxInvocationContext) ctx).addAllAffectedKeys(keys);
   }

   private void acquireRemoteIfNeeded(InvocationContext ctx, DataCommand command, boolean localNodeIsLockOwner) throws Throwable {
      Object key = command.getKey();
      boolean needBackupLocks = ctx.isOriginLocal() && (!localNodeIsLockOwner || isStateTransferInProgress());
      if (needBackupLocks && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         final boolean alreadyLocked = localTransaction.getAffectedKeys().contains(key);
         if (alreadyLocked) {
            log.tracef("We already have lock for key %s, skip remote lock acquisition", key);
            return;
         } else {
            LockControlCommand lcc = cf.buildLockControlCommand(
                  key, command.getFlags(), txContext.getGlobalTransaction());
            invokeNextInterceptor(ctx, lcc);
         }
      }
      ((TxInvocationContext) ctx).addAffectedKey(key);
   }

   private boolean isStateTransferInProgress() {
      return stateTransferManager != null && stateTransferManager.isStateTransferInProgress();
   }

   private void releaseLocksOnFailureBeforePrepare(InvocationContext ctx) {
      lockManager.unlockAll(ctx);
      if (ctx.isOriginLocal() && ctx.isInTxScope() && rpcManager != null) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         TxCompletionNotificationCommand command = cf.buildTxCompletionNotificationCommand(null, txContext.getGlobalTransaction());
         final LocalTransaction cacheTransaction = (LocalTransaction) txContext.getCacheTransaction();
         rpcManager.invokeRemotely(cacheTransaction.getRemoteLocksAcquired(), command, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE));
      }
   }

   private void lockAndRegisterBackupLock(TxInvocationContext ctx,
         Object key, boolean isLockOwner, long lockTimeout, boolean skipLocking) throws InterruptedException {
      if (isLockOwner) {
         lockKeyAndCheckOwnership(ctx, key, lockTimeout, skipLocking);
      } else if (cdl.localNodeIsOwner(key)) {
         ctx.getCacheTransaction().addBackupLockForKey(key);
      }
   }
}
