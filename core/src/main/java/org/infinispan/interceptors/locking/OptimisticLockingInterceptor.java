package org.infinispan.interceptors.locking;

import java.util.Collection;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {
   private static final Log log = LogFactory.getLog(OptimisticLockingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final Collection<?> keysToLock = command.getKeysToLock();
      ((TxInvocationContext<?>) ctx).addAllAffectedKeys(command.getAffectedKeys());
      if (!keysToLock.isEmpty()) {
         if (command.isRetriedCommand() && ctx.isOriginLocal()) {
            //clear backup locks for local and retried commands only. The remote commands clears the backup locks in PendingTxAction.
            ctx.getCacheTransaction().cleanupBackupLocks();
            keysToLock.removeAll(ctx.getLockedKeys()); //already locked!
         }
         lockAllOrRegisterBackupLock(ctx, keysToLock, cacheConfiguration.locking().lockAcquisitionTimeout());
      }

      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> releaseLockOnTxCompletion(((TxInvocationContext) rCtx)));

   }

   @Override
   protected Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys,
         int topologyId) {
      return invokeNext(ctx, command);
   }

   @Override
   protected <K> Object handleWriteManyCommand(InvocationContext ctx, FlagAffectedCommand command,
         Collection<K> keys, boolean forwarded, int topologyId) throws Throwable {
      // TODO: can locks be acquired here with optimistic locking at all? Shouldn't we unlock only when exception is thrown?
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new InvalidCacheUsageException("Explicit locking is not allowed with optimistic caches!");
   }
}
