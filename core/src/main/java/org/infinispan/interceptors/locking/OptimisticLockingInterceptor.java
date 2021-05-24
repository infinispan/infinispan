package org.infinispan.interceptors.locking;

import java.util.Collection;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {
   private static final Log log = LogFactory.getLog(OptimisticLockingInterceptor.class);
   private final InvocationFinallyAction<VisitableCommand> releaseLockOnCompletionAction = (rCtx, rCommand, rv, throwable) -> releaseLockOnTxCompletion((TxInvocationContext<?>) rCtx);
   private final InvocationSuccessFunction<PrepareCommand> onePhaseCommitFunction = (rCtx, rCommand, rv) -> invokeNextAndFinally(rCtx, rCommand, releaseLockOnCompletionAction);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitPrepareCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final Collection<?> keysToLock = command.getKeysToLock();
      InvocationStage lockStage = InvocationStage.completedNullStage();
      ((TxInvocationContext<?>) ctx).addAllAffectedKeys(command.getAffectedKeys());
      if (!keysToLock.isEmpty()) {
         if (command.isRetriedCommand()) {
            // Don't keep backup locks if the local node is the primary owner in the current topology
            // The lock/prepare command is being retried, so it's not a "pending" transaction
            // However, keep the backup locks if the prepare command is being replayed because of state transfer
            ctx.getCacheTransaction().cleanupBackupLocks();
         }
         lockStage = lockAllOrRegisterBackupLock(ctx, command, keysToLock, command.hasZeroLockAcquisition() ? 0 :
               cacheConfiguration.locking().lockAcquisitionTimeout());
      }

      if (command.isOnePhaseCommit()) {
         return lockStage.thenApply(ctx, command, onePhaseCommitFunction);
      } else {
         return makeStage(asyncInvokeNext(ctx, command, lockStage))
               .andExceptionally(ctx, command, lockLeakCheck);
      }
   }

   @Override
   protected Object visitDataReadCommand(InvocationContext ctx, DataCommand command) {
      return invokeNext(ctx, command);
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) {
      return invokeNext(ctx, command);
   }

   @Override
   protected <K> Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command,
                                               Collection<K> keys, boolean forwarded) throws Throwable {
      // TODO: can locks be acquired here with optimistic locking at all? Shouldn't we unlock only when exception is thrown?
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public Object visitLockControlCommand(@SuppressWarnings("rawtypes") TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new InvalidCacheUsageException("Explicit locking is not allowed with optimistic caches!");
   }
}
