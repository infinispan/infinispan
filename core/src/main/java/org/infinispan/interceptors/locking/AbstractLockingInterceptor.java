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
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockUtil;
import org.infinispan.util.function.TetraConsumer;
import org.infinispan.util.logging.Log;

/**
 * Base class for various locking interceptors in this package.
 *
 * @author Mircea Markus
 */
public abstract class AbstractLockingInterceptor extends DDAsyncInterceptor {
   private final boolean trace = getLog().isTraceEnabled();

   protected LockManager lockManager;
   protected DataContainer<Object, Object> dataContainer;
   protected ClusteringDependentLogic cdl;

   protected final TetraConsumer<InvocationContext, VisitableCommand, Object, Throwable> unlockAllReturnHandler = new TetraConsumer<InvocationContext, VisitableCommand, Object, Throwable>() {
      @Override
      public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) {
         lockManager.unlockAll(rCtx);
      }
   };

   protected abstract Log getLog();

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer<Object, Object> dataContainer,
                               ClusteringDependentLogic cdl) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
   }

   @Override
   public final InvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public InvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   protected abstract InvocationStage visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable;

   protected abstract InvocationStage visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable;

   // We need this method in here because of putForExternalRead
   protected final InvocationStage visitNonTxDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (hasSkipLocking(command) || !shouldLockKey(command.getKey())) {
         return invokeNext(ctx, command);
      }

      try {
         lockAndRecord(ctx, command.getKey(), getLockTimeoutMillis(command));
      } catch (Throwable t) {
         lockManager.unlockAll(ctx);
         throw t;
      }
      return invokeNext(ctx, command)
            .whenComplete(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public final InvocationStage visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }
      try {
         lockAllAndRecord(ctx, Arrays.asList(command.getKeys()), getLockTimeoutMillis(command));
      } catch (Throwable t) {
         lockManager.unlockAll(ctx);
      }
      return invokeNext(ctx, command)
            .whenComplete(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public final InvocationStage visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      if (command.isCausedByALocalWrite(cdl.getAddress())) {
         if (trace) getLog().trace("Skipping invalidation as the write operation originated here.");
         return completedStage(null);
      }

      if (hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }

      final Object[] keys = command.getKeys();
      if (keys == null || keys.length < 1) {
         return completedStage(null);
      }

      ArrayList<Object> keysToInvalidate = new ArrayList<>(keys.length);
      for (Object key : keys) {
         try {
            lockAndRecord(ctx, key, 0);
            keysToInvalidate.add(key);
         } catch (TimeoutException te) {
            getLog().unableToLockToInvalidate(key, cdl.getAddress());
         }
      }
      if (keysToInvalidate.isEmpty()) {
         return completedStage(null);
      }

      command.setKeys(keysToInvalidate.toArray());
      return invokeNext(ctx, command).whenComplete(ctx, command, (rCtx, rCommand, rv, t) -> {
         ((InvalidateL1Command) rCommand).setKeys(keys);
         if (!rCtx.isInTxScope()) lockManager.unlockAll(rCtx);
      });
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getMap().keySet(), command.isForwarded());
   }

   @Override
   public InvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public InvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public InvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   @Override
   public InvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getAffectedKeys(), command.isForwarded());
   }

   protected abstract <K> InvocationStage handleWriteManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<K> keys, boolean forwarded) throws Throwable;

   protected final Throwable cleanLocksAndRethrow(InvocationContext ctx, Throwable te) {
      lockManager.unlockAll(ctx);
      return te;
   }

   protected final long getLockTimeoutMillis(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT) ? 0 :
            cacheConfiguration.locking().lockAcquisitionTimeout();
   }

   protected final boolean shouldLockKey(Object key) {
      //only the primary owner acquires the lock.
      boolean shouldLock = LockUtil.isLockOwner(key, cdl);
      if (trace) getLog().tracef("Are (%s) we the lock owners for key '%s'? %s", cdl.getAddress(), toStr(key), shouldLock);
      return shouldLock;
   }

   protected final void lockAndRecord(InvocationContext context, Object key, long timeout) throws InterruptedException {
      context.addLockedKey(key);
      lockManager.lock(key, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS).lock();
   }

   protected final void lockAllAndRecord(InvocationContext context, Stream<?> keys, long timeout) throws InterruptedException {
      lockAllAndRecord(context, keys.collect(Collectors.toList()), timeout);
   }

   protected final void lockAllAndRecord(InvocationContext context, Collection<?> keys, long timeout) throws InterruptedException {
      keys.forEach(context::addLockedKey);
      lockManager.lockAll(keys, context.getLockOwner(), timeout, TimeUnit.MILLISECONDS).lock();
   }

   protected final boolean hasSkipLocking(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }
}
