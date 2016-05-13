package org.infinispan.interceptors.locking;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Locking interceptor to be used by optimistic transactional caches.
 *
 * @author Mircea Markus
 */
public class OptimisticLockingInterceptor extends AbstractTxLockingInterceptor {

   private boolean needToMarkReads;

   private static final Log log = LogFactory.getLog(OptimisticLockingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Start
   public void start() {
      needToMarkReads = cacheConfiguration.clustering().cacheMode() == CacheMode.LOCAL &&
            cacheConfiguration.locking().writeSkewCheck() &&
            cacheConfiguration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ &&
            !cacheConfiguration.unsafe().unreliableReturnValues();
   }

   private void markKeyAsRead(InvocationContext ctx, DataCommand command, boolean forceRead) {
      if (needToMarkReads && ctx.isInTxScope() &&
            (forceRead || !command.hasFlag(Flag.IGNORE_RETURN_VALUES))) {
         TxInvocationContext tctx = (TxInvocationContext) ctx;
         tctx.getCacheTransaction().addReadKey(command.getKey());
      }
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final Collection<Object> keysToLock = command.getKeysToLock();
      ((TxInvocationContext<?>) ctx).addAllAffectedKeys(command.getAffectedKeys());
      if (!keysToLock.isEmpty()) {
         if (command.isRetriedCommand() && ctx.isOriginLocal()) {
            //clear backup locks for local and retried commands only. The remote commands clears the backup locks in PendingTxAction.
            ctx.getCacheTransaction().cleanupBackupLocks();
            keysToLock.removeAll(ctx.getLockedKeys()); //already locked!
         }
         Collection<Object> lockedKeys = lockAllOrRegisterBackupLock(ctx, keysToLock,
                                                                     cacheConfiguration.locking().lockAcquisitionTimeout());
         if (!lockedKeys.isEmpty()) {
            for (Object key : lockedKeys) {
               performLocalWriteSkewCheck(ctx, key);
            }
         }
      }

      return ctx.shortCircuit(invokeNextAndCommitIf1Pc(ctx, command));
   }

   @Override
   protected CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      markKeyAsRead(ctx, command, true);
      try {
         return ctx.shortCircuit(ctx.forkInvocationSync(command));
      } finally {
         //when not invoked in an explicit tx's scope the get is non-transactional(mainly for efficiency).
         //locks need to be released in this situation as they might have been acquired from L1.
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      markKeyAsRead(ctx, command, true);
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (needToMarkReads && ctx.isInTxScope()) {
         TxInvocationContext tctx = (TxInvocationContext) ctx;
         for (Object key : command.getKeys()) {
            tctx.getCacheTransaction().addReadKey(key);
         }
      }
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      try {
         return ctx.shortCircuit(ctx.forkInvocationSync(command));
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      try {
         return ctx.shortCircuit(ctx.forkInvocationSync(command));
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   protected CompletableFuture<Void> visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      try {
         // Regardless of whether is conditional so that
         // write skews can be detected in both cases.
         markKeyAsRead(ctx, command, command.isConditional());
         return ctx.shortCircuit(ctx.forkInvocationSync(command));
      } catch (Throwable te) {
         throw cleanLocksAndRethrow(ctx, te);
      }
   }

   @Override
   public CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new InvalidCacheUsageException("Explicit locking is not allowed with optimistic caches!");
   }

   private void performLocalWriteSkewCheck(TxInvocationContext ctx, Object key) {
      CacheEntry ce = ctx.lookupEntry(key);
      if (ce instanceof RepeatableReadEntry && ctx.getCacheTransaction().keyRead(key)) {
         if (trace) {
            log.tracef("Performing local write skew check for key %s", toStr(key));
         }
         ((RepeatableReadEntry) ce).performLocalWriteSkewCheck(dataContainer, true);
      } else {
         if (trace) {
            log.tracef("*Not* performing local write skew check for key %s", toStr(key));
         }
      }
   }

}
