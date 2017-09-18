package org.infinispan.interceptors.locking;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used by pessimistic transactions.
 * <p>
 * Design (from 9.1):
 * <p>
 * This interceptor is skipped when {@link Flag#SKIP_LOCKING} is used or when all affected keys are already locked.
 * Read-only commands can acquire the locks when {@link Flag#FORCE_WRITE_LOCK}.
 * <p>
 * The new design has 2 concepts. The primary lock and the backup lock. The primary lock is acquired in the primary
 * owner and it is exclusive; only one transaction can acquire it. The backup lock is acquired by the backup owner and
 * it is shared; multiple transactions can acquire simultaneously. The backup locks are associated with the topology id
 * and they are only used when the primary owner leaves or crashes.
 * <p>
 * This algorithm contacts all owners and acquires the primary lock in the primary owner and the backup lock in the
 * backup owners. So, lock acquisition (single or multiple keys) happens in a single RPC to all the owners of the
 * key(s). This is triggered in {@link TxDistributionInterceptor#visitLockControlCommand(TxInvocationContext,
 * LockControlCommand)}.
 * <p>
 * The {@link Flag#CACHE_MODE_LOCAL} changes this behaviour; it makes the lock acquisition only local. So, only local
 * the primary (if primary owner) or backup lock (if backup owner) is acquired. No RPC is create and if the node isn't
 * an owner, no locks are acquired.
 * <p>
 * <p>
 * In case of primary owner changes, there are 2 cases.
 * <p>
 * 1) a new node joins and it is the new primary owner. In this case, the old primary owner transfers the lock
 * information to the new primary owner.
 * <p>
 * 2) the primary owner crashes or leaves. In this case, a backup owner will be promoted to the primary owner. All
 * primary lock requests are hold until its previous backup locks are released. Note that a transaction only acquires
 * the lock once. If a transaction requests a primary lock but has the backup lock acquired, it doesn't grant the
 * primary lock if there are other backup locks from other transactions.
 * <p>
 * Know Issue: There is small deadlock probably is 2 or more transaction try to acquire the lock of a key and the
 * primary owner dies before any of them receives a reply. In that case, none of the transactions (and future
 * transactions) are able to acquire the primary lock until all backup lock are cleared. Eventually, the transactions
 * will rollback and the backup lock will be released. One way to solve it is to acquire the backup locks after the
 * primary lock. However, this will increase the performance penalty for all locks by generating 2 RPCs.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 */
public class PessimisticLockingInterceptor extends AbstractTxLockingInterceptor {
   private static final Log log = LogFactory.getLog(PessimisticLockingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private CommandsFactory cf;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(CommandsFactory factory) {
      this.cf = factory;
   }

   @Override
   protected final Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      return readNeedsLock(ctx, command) ?
             invokeLockCommandAndContinue(Collections.singleton(command.getKey()), command, (TxInvocationContext<?>) ctx, command.getTopologyId()) :
             invokeNext(ctx, command);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys,
         int topologyId) throws Throwable {
      return readNeedsLock(ctx, command) ?
             invokeLockCommandAndContinue(keys, command, (TxInvocationContext) ctx, topologyId) :
             invokeNext(ctx, command);
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
   protected <K> Object handleWriteManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<K> keys,
         boolean forwarded, int topologyId) throws Throwable {
      final TxInvocationContext<?> txCtx = (TxInvocationContext<?>) ctx;
      if (skipLockingForKeys(command, txCtx, keys)) {
         txCtx.addAllAffectedKeys(keys);
         return invokeNext(ctx, command);
      } else if (isLocalOnly(command)) {
         lockAllOrRegisterBackupLock(txCtx, keys, getLockTimeoutMillis(command));
         txCtx.addAllAffectedKeys(keys);
         return invokeNext(ctx, command);
      } else {
         return invokeLockCommandAndContinue(keys, command, txCtx, topologyId);
      }
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command)
         throws Throwable {
      assert ctx.isInTxScope() : "Unable to handle non-transactional commands";
      final Object key = command.getKey();
      final TxInvocationContext<?> txCtx = (TxInvocationContext<?>) ctx;
      if (skipLockingForKey(command, txCtx, key)) {
         txCtx.addAffectedKey(key);
         return invokeNext(ctx, command);
      } else if (isLocalOnly(command)) {
         lockOrRegisterBackupLock(txCtx, key, getLockTimeoutMillis(command));
         txCtx.addAffectedKey(key);
         return invokeNext(ctx, command);
      } else {
         return invokeLockCommandAndContinue(Collections.singleton(key), command, txCtx, command.getTopologyId());
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      assert ctx.isInTxScope() : "Locks should only be acquired within the scope of a transaction!";

      if (command.isUnlock()) { //TODO double check unlock. it isn't used anymore!
         assert !ctx.isOriginLocal() : "There's no advancedCache.unlock so this must have originated remotely.";
         return false;
      }

      if (hasSkipLocking(command)) {
         throw log.lockCommandWithSkipLocking();
      }

      if (hasSkipLocking(command) || areAllKeysAlreadyLocked(ctx, command.getKeys())) {
         //lock control with skip locking?? that is weird...
         if (trace) {
            log.tracef("Skip locking or keys already locked. Skipping command %s", command);
         }
         return false;
      } else if (isLocalOnly(command)) {
         lockOrRegisterBackupLock(ctx, command.getKeys(), getLockTimeoutMillis(command));
         ctx.addAffectedKey(command.getKeys());
         return invokeNext(ctx, command);
      } else {
         return ctx.isOriginLocal() ?
                handleLocalLockCommand(ctx, command) :
                handleRemoteLockCommand(ctx, command);
      }
   }

   private boolean readNeedsLock(InvocationContext ctx, FlagAffectedCommand command) {
      if (ctx.isInTxScope() && ctx.isOriginLocal()) {
         boolean forceWriteLock = command.hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK);
         if (forceWriteLock && hasSkipLocking(command)) {
            throw log.invalidForceWriteLockAndSkipLockingFlags();
         }
         return forceWriteLock;
      } else {
         return false;
      }
   }

   private boolean areAllKeysAlreadyLocked(TxInvocationContext<?> context, Collection<?> keys) {
      return context.getAffectedKeys().containsAll(keys);
   }

   private boolean isKeysAlreadyLocked(TxInvocationContext<?> context, Object key) {
      return context.getAffectedKeys().contains(key);
   }

   private Object handleRemoteLockCommand(TxInvocationContext context, LockControlCommand command)
         throws InterruptedException {
      lockAllOrRegisterBackupLock(context, command.getKeys(), getLockTimeoutMillis(command));
      return invokeNext(context, command);
   }

   private boolean skipLockingForKey(FlagAffectedCommand command, TxInvocationContext<?> ctx, Object key) {
      return !ctx.isOriginLocal() ||
             hasSkipLocking(command) ||
             isKeysAlreadyLocked(ctx, key);
   }

   private boolean skipLockingForKeys(FlagAffectedCommand command, TxInvocationContext<?> ctx,
         Collection<?> keys) {
      return !ctx.isOriginLocal() ||
             hasSkipLocking(command) ||
             areAllKeysAlreadyLocked(ctx, keys);
   }

   private boolean isLocalOnly(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
   }

   private Object invokeLockCommandAndContinue(Collection<?> keys, FlagAffectedCommand command,
         TxInvocationContext<?> ctx, int topologyId) throws InterruptedException {
      //we need to acquire locks
      LockControlCommand lcc = cf.buildLockControlCommand(keys, command.getFlagsBitSet(), ctx.getGlobalTransaction());
      lcc.setTopologyId(topologyId);
      return makeStage(handleLocalLockCommand(ctx, lcc))
            .thenApply(ctx, command, (rCtx, rCommand, rv) -> invokeNext(rCtx, rCommand));
   }

   private Object handleLocalLockCommand(TxInvocationContext context, LockControlCommand command)
         throws InterruptedException {
      //TODO blocking method. change it to non-blocking? [minor, only blocks local commands]
      lockAllOrRegisterBackupLock(context, command.getKeys(), getLockTimeoutMillis(command));
      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext<?> ctx = (TxInvocationContext) rCtx;
         LockControlCommand cmd = (LockControlCommand) rCommand;
         ctx.addAllAffectedKeys(cmd.getKeys());
         return true;
      });
   }


}
