package org.infinispan.interceptors.locking;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.concurrent.locks.LockUtil;
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

   private CommandsFactory cf;
   private StateTransferManager stateTransferManager;

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
   protected final BasicInvocationStage visitDataReadCommand(InvocationContext ctx, DataCommand command)
         throws Throwable {
      if (!readNeedsLock(ctx, command)) {
         return invokeNext(ctx, command);
      }

      try {
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
         return invokeNext(ctx, lcc).thenCompose((stage, rCtx, rCommand, rv) -> invokeNext(rCtx, command)).handle(
               (rCtx, rCommand, rv, t) -> {
                  if (t != null) {
                     rethrowAndReleaseLocksIfNeeded(rCtx, t);
                     return;
                  }

                  acquireLocalLock(rCtx, command);

                  // TODO This was probably needed at some time for L1 writes, but not now
                  if (!rCtx.isInTxScope()) {
                     lockManager.unlockAll(rCtx);
                  }
               });
      } catch (Throwable t) {
         rethrowAndReleaseLocksIfNeeded(ctx, t);
         throw t;
      }

   }

   private boolean readNeedsLock(InvocationContext ctx, LocalFlagAffectedCommand command) {
      return ctx.isInTxScope() && command.hasFlag(Flag.FORCE_WRITE_LOCK) && !hasSkipLocking(command);
   }

   private void acquireLocalLock(InvocationContext ctx, DataCommand command) throws InterruptedException {
      log.tracef("acquireLocalLock");
      final TxInvocationContext txContext = (TxInvocationContext) ctx;
      Object key = command.getKey();
      lockOrRegisterBackupLock(txContext, key, getLockTimeoutMillis(command));
      txContext.addAffectedKey(key);
   }

   @Override
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      try {
         InvocationStage stage;
         if (!readNeedsLock(ctx, command)) {
            stage = invokeNext(ctx, command);
         } else {
            Collection<?> keys = command.getKeys();
            if (!needRemoteLocks(ctx, keys, command)) {
               acquireLocalLocks(ctx, command, keys);
               stage = invokeNext(ctx, command);
            } else {
               final TxInvocationContext txContext = (TxInvocationContext) ctx;
               LockControlCommand lcc = cf.buildLockControlCommand(keys, command.getFlagsBitSet(),
                     txContext.getGlobalTransaction());
               stage = invokeNext(ctx, lcc).thenCompose((stage1, rCtx, rCommand, rv) -> {
                  acquireLocalLocks(rCtx, (LocalFlagAffectedCommand) rCommand, keys);
                  return invokeNext(rCtx, command);
               });
            }
         }
         return stage.handle((rCtx, rCommand, rv, t) -> {
            if (t != null) {
               releaseLocksOnFailureBeforePrepare(rCtx);
            }
         });
      } catch (Throwable t) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw t;
      }
   }

   private void acquireLocalLocks(InvocationContext ctx, LocalFlagAffectedCommand command, Collection<?> keys)
         throws InterruptedException {
      lockAllOrRegisterBackupLock((TxInvocationContext<?>) ctx, keys, getLockTimeoutMillis(command));
      ((TxInvocationContext<?>) ctx).addAllAffectedKeys(keys);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }

      // Don't release the locks on exception, the RollbackCommand will do it
      return invokeNext(ctx, command).thenAccept(
            (rCtx, rCommand, rv) -> releaseLockOnTxCompletion(((TxInvocationContext) rCtx)));
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         InvocationStage stage;
         if (hasSkipLocking(command)) {
            stage = invokeNext(ctx, command);
         } else {
            final Collection<Object> affectedKeys = command.getMap().keySet();
            if (!needRemoteLocks(ctx, affectedKeys, command)) {
               acquireLocalLocks(ctx, command, affectedKeys);
               stage = invokeNext(ctx, command);
            } else {
               final TxInvocationContext txContext = (TxInvocationContext) ctx;
               LockControlCommand lcc = cf.buildLockControlCommand(affectedKeys, command.getFlagsBitSet(),
                     txContext.getGlobalTransaction());
               stage = invokeNext(ctx, lcc).thenCompose((stage1, rCtx, rCommand, rv) -> {
                  acquireLocalLocks(rCtx, (LocalFlagAffectedCommand) rCommand, affectedKeys);
                  return invokeNext(rCtx, command);
               });
            }
         }
         return stage.handle((rCtx, rCommand, rv, t) -> {
            if (t != null) {
               rethrowAndReleaseLocksIfNeeded(rCtx, t);
            }
         });
      } catch (Throwable t) {
         rethrowAndReleaseLocksIfNeeded(ctx, t);
         throw t;
      }
   }

   @Override
   protected BasicInvocationStage visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command)
         throws Throwable {
      try {
         InvocationStage stage;
         Object key = command.getKey();
         if (hasSkipLocking(command)) {
            // Mark the key as affected even with SKIP_LOCKING
            ((TxInvocationContext<?>) ctx).addAffectedKey(key);
            stage = invokeNext(ctx, command);
         } else {
            if (!needRemoteLocks(ctx, key, command)) {
               acquireLocalLock(ctx, command);
               stage = invokeNext(ctx, command);
            } else {
               final TxInvocationContext txContext = (TxInvocationContext) ctx;
               LockControlCommand lcc = cf.buildLockControlCommand(key, command.getFlagsBitSet(),
                     txContext.getGlobalTransaction());
               return invokeNext(ctx, lcc).compose((stage1, rCtx, rCommand, rv, t) -> {
                  rethrowAndReleaseLocksIfNeeded(rCtx, t);
                  acquireLocalLock(rCtx, command);
                  return invokeNext(rCtx, command);
               });
            }
         }
         return stage.handle((rCtx, rCommand, rv, t) -> {
            if (t != null) {
               rethrowAndReleaseLocksIfNeeded(rCtx, t);
            }
         });
      } catch (Throwable t) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw t;
      }
   }

   @Override
   public BasicInvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      try {
         InvocationStage stage;
         if (hasSkipLocking(command)) {
            stage = invokeNext(ctx, command);
         } else {
            Object[] compositeKeys = command.getCompositeKeys();
            Set<Object> keysToLock = new HashSet<>(Arrays.asList(compositeKeys));
            if (!needRemoteLocks(ctx, keysToLock, command)) {
               acquireLocalCompositeLocks(command, keysToLock, ctx);
               stage = invokeNext(ctx, command);
            } else {
               final TxInvocationContext txContext = (TxInvocationContext) ctx;
               LockControlCommand lcc = cf.buildLockControlCommand(keysToLock, command.getFlagsBitSet(),
                     txContext.getGlobalTransaction());
               stage = invokeNext(ctx, lcc).thenCompose((stage1, rCtx, rCommand, rv) -> {
                  ((TxInvocationContext<?>) rCtx).addAllAffectedKeys(keysToLock);
                  acquireLocalCompositeLocks(command, keysToLock, rCtx);
                  return invokeNext(rCtx, command);
               });
            }
         }
         return stage.handle((rCtx, rCommand, rv, t) -> {
            if (t != null) {
               lockManager.unlockAll(rCtx);
            }
         });
      } catch (Throwable t) {
         lockManager.unlockAll(ctx);
         throw t;
      }
   }

   private void acquireLocalCompositeLocks(ApplyDeltaCommand command, Set<Object> keysToLock,
         InvocationContext ctx1) throws InterruptedException {
      if (cdl.localNodeIsPrimaryOwner(command.getKey())) {
         lockAllAndRecord(ctx1, keysToLock, getLockTimeoutMillis(command));
      } else if (cdl.localNodeIsOwner(command.getKey())) {
         TxInvocationContext<?> txContext = (TxInvocationContext<?>) ctx1;
         for (Object key : keysToLock) {
            txContext.getCacheTransaction().addBackupLockForKey(key);
         }
      }
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (!ctx.isInTxScope())
         throw new IllegalStateException("Locks should only be acquired within the scope of a transaction!");

      boolean skipLocking = hasSkipLocking(command);
      if (skipLocking) {
         return returnWith(false);
      }

      // First go through the distribution interceptor to acquire the remote lock - required by DLD.
      // Only acquire remote lock if multiple keys or the single key primary owner is not the local node.
      if (ctx.isOriginLocal()) {
         final boolean isSingleKeyAndLocal =
               !command.multipleKeys() && cdl.localNodeIsPrimaryOwner(command.getSingleKey());
         boolean needBackupLocks = !isSingleKeyAndLocal || isStateTransferInProgress();
         if (needBackupLocks && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
            LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
            if (localTx.getAffectedKeys().containsAll(command.getKeys())) {
               if (trace)
                  log.tracef("Already own locks on keys: %s, skipping remote call", command.getKeys());
               return returnWith(true);
            }
         } else {
            if (trace)
               log.tracef("Single key %s and local, skipping remote call", command.getSingleKey());
            return returnWith(localLockCommandWork(ctx, command));
         }
      }

      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
         rethrowAndReleaseLocksIfNeeded(rCtx, t);
         return returnWith(localLockCommandWork(rCtx, (LockControlCommand) rCommand));
      });
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
         releaseLocksOnFailureBeforePrepare(ctx);
         return false;
      }

      try {
         lockAllOrRegisterBackupLock(txInvocationContext, command.getKeys(), getLockTimeoutMillis(command));
      } catch (Throwable t) {
         releaseLocksOnFailureBeforePrepare(ctx);
         throw t;
      }
      return true;
   }

   private void rethrowAndReleaseLocksIfNeeded(InvocationContext ctx, Throwable throwable)
         throws Throwable {
      if (throwable != null) {
         // If the command will be retried, there is no need to release this or other locks
         if (!(throwable instanceof OutdatedTopologyException)) {
            releaseLocksOnFailureBeforePrepare(ctx);
         }
         throw throwable;
      }
   }

   private boolean needRemoteLocks(InvocationContext ctx, Collection<?> keys,
         LocalFlagAffectedCommand command) throws Throwable {
      boolean needBackupLocks = ctx.isOriginLocal() && (!isLockOwner(keys) || isStateTransferInProgress());
      boolean needRemoteLock = false;
      if (needBackupLocks && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         needRemoteLock = !localTransaction.getAffectedKeys().containsAll(keys);
         if (!needRemoteLock) {
            log.tracef("We already have lock for keys %s, skip remote lock acquisition", keys);
         }
      }
      return needRemoteLock;
   }

   private boolean needRemoteLocks(InvocationContext ctx, Object key, LocalFlagAffectedCommand command)
         throws Throwable {
      boolean needBackupLocks = ctx.isOriginLocal() && (!isLockOwner(key) || isStateTransferInProgress());
      boolean needRemoteLock = false;
      if (needBackupLocks && !command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         final TxInvocationContext txContext = (TxInvocationContext) ctx;
         LocalTransaction localTransaction = (LocalTransaction) txContext.getCacheTransaction();
         needRemoteLock = !localTransaction.getAffectedKeys().contains(key);
         if (!needRemoteLock) {
            log.tracef("We already have lock for key %s, skip remote lock acquisition", key);
         }
      }
      return needRemoteLock;
   }

   private boolean isLockOwner(Collection<?> keys) {
      for (Object key : keys) {
         if (LockUtil.getLockOwnership(key, cdl) != LockUtil.LockOwnership.PRIMARY) {
            return false;
         }
      }
      return true;
   }

   private boolean isLockOwner(Object key) {
      return LockUtil.getLockOwnership(key, cdl) == LockUtil.LockOwnership.PRIMARY;
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
         rpcManager.invokeRemotely(cacheTransaction.getRemoteLocksAcquired(), command, rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE));
      }
   }

}
