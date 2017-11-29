package org.infinispan.interceptors.locking;

import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.logging.Log;

/**
 * Base class for various locking interceptors in this package.
 *
 * @author Mircea Markus
 */
public abstract class AbstractLockingInterceptor extends DDAsyncInterceptor {
   private final boolean trace = getLog().isTraceEnabled();

   protected LockManager lockManager;
   protected ClusteringDependentLogic cdl;

   final InvocationFinallyAction unlockAllReturnHandler = new InvocationFinallyAction() {
      @Override
      public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         lockManager.unlockAll(rCtx);
      }
   };

   protected abstract Log getLog();

   @Inject
   public void setDependencies(LockManager lockManager, ClusteringDependentLogic cdl) {
      this.lockManager = lockManager;
      this.cdl = cdl;
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         // Cache.putForExternalRead() is non-transactional
         return visitNonTxDataWriteCommand(ctx, command);
      }
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   protected abstract Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable;

   protected abstract Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable;

   // We need this method in here because of putForExternalRead
   final Object visitNonTxDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      Object key;
      if (hasSkipLocking(command) || !shouldLockKey((key = command.getKey()))) {
         return invokeNext(ctx, command);
      }

      LockPromise lockPromise = lockAndRecord(ctx, key, getLockTimeoutMillis(command));
      return nonTxLockAndInvokeNext(ctx, command, lockPromise, unlockAllReturnHandler);
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }
      LockPromise lockPromise = lockAllAndRecord(ctx, Arrays.asList(command.getKeys()), getLockTimeoutMillis(command));
      return nonTxLockAndInvokeNext(ctx, command, lockPromise, unlockAllReturnHandler);
   }

   @Override
   public final Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      if (command.isCausedByALocalWrite(cdl.getAddress())) {
         if (trace) getLog().trace("Skipping invalidation as the write operation originated here.");
         return null;
      }

      if (hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }

      final Object[] keys = command.getKeys();
      if (keys == null || keys.length < 1) {
         return null;
      }

      ArrayList<Object> keysToInvalidate = new ArrayList<>(keys.length);
      for (Object key : keys) {
         try {
            //don't make it async. the command is remote anyway.
            lockAndRecord(ctx, key, 0).lock();
            keysToInvalidate.add(key);
         } catch (TimeoutException te) {
            getLog().unableToLockToInvalidate(key, cdl.getAddress());
         }
      }
      if (keysToInvalidate.isEmpty()) {
         return null;
      }

      command.setKeys(keysToInvalidate.toArray());
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         ((InvalidateL1Command) rCommand).setKeys(keys);
         if (!rCtx.isInTxScope()) lockManager.unlockAll(rCtx);
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getMap().keySet(), command.isForwarded());
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleReadManyCommand(ctx, command, command.getKeys());
   }

   protected abstract Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) throws Throwable;

   protected abstract <K> Object handleWriteManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<K> keys, boolean forwarded) throws Throwable;

   protected final long getLockTimeoutMillis(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT) ? 0 :
            cacheConfiguration.locking().lockAcquisitionTimeout();
   }

   final boolean shouldLockKey(Object key) {
      //only the primary owner acquires the lock.
      boolean shouldLock = isLockOwner(key);
      if (trace) getLog().tracef("Are (%s) we the lock owners for key '%s'? %s", cdl.getAddress(), toStr(key), shouldLock);
      return shouldLock;
   }

   final boolean isLockOwner(Object key) {
      return cdl.getCacheTopology().getDistribution(key).isPrimary();
   }

   protected final KeyAwareLockPromise lockAndRecord(InvocationContext context, Object key, long timeout) {
      context.addLockedKey(key);
      return lockManager.lock(key, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS);
   }

   final KeyAwareLockPromise lockAllAndRecord(InvocationContext context, Stream<?> keys, long timeout) {
      return lockAllAndRecord(context, keys.collect(Collectors.toList()), timeout);
   }

   final KeyAwareLockPromise lockAllAndRecord(InvocationContext context, Collection<?> keys, long timeout) {
      keys.forEach(context::addLockedKey);
      return lockManager.lockAll(keys, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS);
   }

   final boolean hasSkipLocking(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   /**
    * Locks and invoke the next interceptor for non-transactional commands.
    */
   protected final Object nonTxLockAndInvokeNext(InvocationContext ctx, VisitableCommand command,
         LockPromise lockPromise, InvocationFinallyAction finallyFunction) {
      return lockPromise.toInvocationStage().andHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            lockManager.unlockAll(rCtx);
            throw throwable;
         } else {
            return invokeNextAndFinally(rCtx, rCommand, finallyFunction);
         }
      });
   }
}
