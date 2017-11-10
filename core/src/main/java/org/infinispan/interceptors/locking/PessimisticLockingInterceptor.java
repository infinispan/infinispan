package org.infinispan.interceptors.locking;

import java.util.Collection;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
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
 */
public class PessimisticLockingInterceptor extends AbstractTxLockingInterceptor {
   private static final Log log = LogFactory.getLog(PessimisticLockingInterceptor.class);
   public static final boolean trace = log.isTraceEnabled();

   private final InvocationSuccessFunction localLockCommandWork =
         (rCtx, rCommand, rv) -> localLockCommandWork(rCtx, (LockControlCommand) rCommand);

   @Inject private CommandsFactory cf;
   @Inject private StateTransferManager stateTransferManager;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected final Object visitDataReadCommand(InvocationContext ctx, DataCommand command)
         throws Throwable {
      if (!readNeedsLock(ctx, command)) {
         return invokeNext(ctx, command);
      }

      if (!readNeedsLock(ctx, command)) {
         return invokeNext(ctx, command);
      }

      Object key = command.getKey();
      if (!needRemoteLocks(ctx, key, command)) {
         acquireLocalLock(ctx, command);
         return invokeNext(ctx, command);
      }

      TxInvocationContext txContext = (TxInvocationContext) ctx;
      LockControlCommand lcc = cf.buildLockControlCommand(key, command.getFlagsBitSet(),
            txContext.getGlobalTransaction());
      lcc.setTopologyId(command.getTopologyId());
      return invokeNextThenApply(ctx, lcc, (rCtx, rCommand, rv) -> {
         acquireLocalLock(rCtx, command);
         return invokeNext(rCtx, command);
      });

   }

   private boolean readNeedsLock(InvocationContext ctx, FlagAffectedCommand command) {
      return ctx.isInTxScope() && command.hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK) && !hasSkipLocking(command);
   }

   private void acquireLocalLock(InvocationContext ctx, DataCommand command) throws InterruptedException {
      if (trace)
         log.tracef("acquireLocalLock");
      final TxInvocationContext txContext = (TxInvocationContext) ctx;
      Object key = command.getKey();
      lockOrRegisterBackupLock(txContext, key, getLockTimeoutMillis(command));
      txContext.addAffectedKey(key);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) throws Throwable {
      Object maybeStage;
      if (!readNeedsLock(ctx, command)) {
         maybeStage = invokeNext(ctx, command);
      } else {
         if (!needRemoteLocks(ctx, keys, command)) {
            acquireLocalLocks(ctx, command, keys);
            maybeStage = invokeNext(ctx, command);
         } else {
            // Acquire the remote locks first, then the local locks
            final TxInvocationContext txContext = (TxInvocationContext) ctx;
            LockControlCommand lcc = cf.buildLockControlCommand(keys, command.getFlagsBitSet(),
                  txContext.getGlobalTransaction());
            maybeStage = invokeNextThenApply(ctx, lcc, (rCtx, rLockCommand, rv) -> {
               acquireLocalLocks(rCtx, command, keys);
               return invokeNext(rCtx, command);
            });
         }
      }
      return maybeStage;
   }

   private void acquireLocalLocks(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys)
         throws InterruptedException {
      lockAllOrRegisterBackupLock((TxInvocationContext<?>) ctx, keys, getLockTimeoutMillis(command));
      ((TxInvocationContext<?>) ctx).addAllAffectedKeys(keys);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }

      // Don't release the locks on exception, the RollbackCommand will do it
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> releaseLockOnTxCompletion(((TxInvocationContext) rCtx)));
   }

   @Override
   protected <K> Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command, Collection<K> keys, boolean forwarded) throws Throwable {
      Object maybeStage;
      if (hasSkipLocking(command)) {
         maybeStage = invokeNext(ctx, command);
      } else {
         if (!needRemoteLocks(ctx, keys, command)) {
            acquireLocalLocks(ctx, command, keys);
            maybeStage = invokeNext(ctx, command);
         } else {
            final TxInvocationContext txContext = (TxInvocationContext) ctx;
            LockControlCommand lcc = cf.buildLockControlCommand(keys, command.getFlagsBitSet(),
                  txContext.getGlobalTransaction());
            lcc.setTopologyId(command.getTopologyId());
            maybeStage = invokeNextThenApply(ctx, lcc, (rCtx, rCommand, rv) -> {
               acquireLocalLocks(rCtx, command, keys);
               return invokeNext(rCtx, command);
            });
         }
      }
      return maybeStage;
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command)
         throws Throwable {
      Object maybeStage;
      Object key = command.getKey();
      if (hasSkipLocking(command)) {
         // Non-modifying functional write commands are executed in non-transactional context on non-originators
         if (ctx.isInTxScope()) {
            // Mark the key as affected even with SKIP_LOCKING
            ((TxInvocationContext<?>) ctx).addAffectedKey(key);
         }
         maybeStage = invokeNext(ctx, command);
      } else {
         if (!needRemoteLocks(ctx, key, command)) {
            acquireLocalLock(ctx, command);
            maybeStage = invokeNext(ctx, command);
         } else {
            final TxInvocationContext txContext = (TxInvocationContext) ctx;
            LockControlCommand lcc = cf.buildLockControlCommand(key, command.getFlagsBitSet(),
                  txContext.getGlobalTransaction());
            lcc.setTopologyId(command.getTopologyId());
            return invokeNextThenApply(ctx, lcc, (rCtx, rCommand, rv) -> {
               acquireLocalLock(rCtx, command);
               return invokeNext(rCtx, command);
            });
         }
      }
      return maybeStage;
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (!ctx.isInTxScope())
         throw new IllegalStateException("Locks should only be acquired within the scope of a transaction!");

      boolean skipLocking = hasSkipLocking(command);
      if (skipLocking) {
         return false;
      }

      // First go through the distribution interceptor to acquire the remote lock - required by DLD.
      // Only acquire remote lock if multiple keys or the single key primary owner is not the local node.
      if (ctx.isOriginLocal()) {
         final boolean isSingleKeyAndLocal =
               !command.multipleKeys() && cdl.getCacheTopology().getDistribution(command.getSingleKey()).isPrimary();
         boolean needBackupLocks = !isSingleKeyAndLocal || isStateTransferInProgress();
         if (needBackupLocks && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
            LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
            if (localTx.getAffectedKeys().containsAll(command.getKeys())) {
               if (trace)
                  log.tracef("Already own locks on keys: %s, skipping remote call", command.getKeys());
               return true;
            }
         } else {
            if (trace)
               log.tracef("Single key %s and local, skipping remote call", command.getSingleKey());
            return localLockCommandWork(ctx, command);
         }
      }

      return invokeNextThenApply(ctx, command, localLockCommandWork);
   }

   private boolean localLockCommandWork(InvocationContext ctx, LockControlCommand command)
         throws InterruptedException {
      TxInvocationContext<?> txInvocationContext = (TxInvocationContext<?>) ctx;
      if (ctx.isOriginLocal()) {
         txInvocationContext.addAllAffectedKeys(command.getKeys());
      }

      if (command.isUnlock()) {
         if (ctx.isOriginLocal()) throw new AssertionError(
               "There's no advancedCache.unlock so this must have originated remotely.");
         return false;
      }

      lockAllOrRegisterBackupLock(txInvocationContext, command.getKeys(), getLockTimeoutMillis(command));
      return true;
   }

   private boolean needRemoteLocks(InvocationContext ctx, Collection<?> keys,
         FlagAffectedCommand command) throws Throwable {
      boolean needBackupLocks = ctx.isOriginLocal() && (!isLockOwner(keys) || isStateTransferInProgress());
      boolean needRemoteLock = false;
      if (needBackupLocks && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         needRemoteLock = !localTransaction.getAffectedKeys().containsAll(keys);
         if (!needRemoteLock) {
            if (trace) log.tracef("We already have lock for keys %s, skip remote lock acquisition", keys);
         }
      }
      return needRemoteLock;
   }

   private boolean needRemoteLocks(InvocationContext ctx, Object key, FlagAffectedCommand command)
         throws Throwable {
      boolean needBackupLocks = ctx.isOriginLocal() && (!isLockOwner(key) || isStateTransferInProgress());
      boolean needRemoteLock = false;
      if (needBackupLocks && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         needRemoteLock = !localTransaction.getAffectedKeys().contains(key);
         if (!needRemoteLock) {
            if (trace)
               log.tracef("We already have lock for key %s, skip remote lock acquisition", key);
         }
      } else {
         if (trace)
            log.tracef("Don't need backup locks %s", needBackupLocks);
      }
      return needRemoteLock;
   }

   private boolean isLockOwner(Collection<?> keys) {
      for (Object key : keys) {
         if (!isLockOwner(key)) {
            return false;
         }
      }
      return true;
   }

   private boolean isStateTransferInProgress() {
      return stateTransferManager != null && stateTransferManager.isStateTransferInProgress();
   }
}
