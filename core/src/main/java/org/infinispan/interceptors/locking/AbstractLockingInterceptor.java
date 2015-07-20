package org.infinispan.interceptors.locking;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Base class for various locking interceptors in this package.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractLockingInterceptor extends CommandInterceptor {

   protected LockManager lockManager;
   protected DataContainer<Object, Object> dataContainer;
   protected ClusteringDependentLogic cdl;

   @Inject
   public void setDependencies(LockManager lockManager, DataContainer<Object, Object> dataContainer,
                               ClusteringDependentLogic cdl) {
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
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
   protected final Object visitNonTxDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      try {
         if (hasSkipLocking(command) || !shouldLockKey(command.getKey())) {
            return invokeNextInterceptor(ctx, command);
         }
         lockAndRecord(ctx, command.getKey(), getLockTimeoutMillis(command));
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public final Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      try {
         if (hasSkipLocking(command)) {
            return invokeNextInterceptor(ctx, command);
         }
         lockAllAndRecord(ctx, Arrays.asList(command.getKeys()), getLockTimeoutMillis(command));
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) {
            lockManager.unlockAll(ctx);
         }
      }
   }

   @Override
   public final Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      if (command.isCausedByALocalWrite(cdl.getAddress())) {
         getLog().trace("Skipping invalidation as the write operation originated here.");
         return null;
      }

      if (hasSkipLocking(command)) {
         return invokeNextInterceptor(ctx, command);
      }

      final Object[] keys = command.getKeys();
      try {
         if (keys != null && keys.length >= 1) {
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
               return null;
            }
            command.setKeys(keysToInvalidate.toArray());
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         command.setKeys(keys);
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   protected final Throwable cleanLocksAndRethrow(InvocationContext ctx, Throwable te) {
      lockManager.unlockAll(ctx);
      return te;
   }

   protected final long getLockTimeoutMillis(LocalFlagAffectedCommand command) {
      return command.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ? 0 : cacheConfiguration.locking().lockAcquisitionTimeout();
   }

   protected final boolean shouldLockKey(Object key) {
      //only the primary owner acquires the lock.
      boolean shouldLock = LockUtil.getLockOwnership(key, cdl) == LockUtil.LockOwnership.PRIMARY;
      getLog().tracef("Are (%s) we the lock owners for key '%s'? %s", cdl.getAddress(), key, shouldLock);
      return shouldLock;
   }

   protected final void lockAndRecord(InvocationContext context, Object key, long timeout) throws InterruptedException {
      lockManager.acquireLock(context, key, timeout, false);
   }

   protected final void lockAllAndRecord(InvocationContext context, Stream<?> keys, long timeout) throws InterruptedException {
      for (Iterator<?> iterator = keys.iterator(); iterator.hasNext();) {
         lockManager.acquireLock(context, iterator.next(), timeout, false);
      }
   }

   protected final void lockAllAndRecord(InvocationContext context, Collection<?> keys, long timeout) throws InterruptedException {
      if (keys.isEmpty()) {
         return;
      }
      for (Object key : keys) {
         lockManager.acquireLock(context, key, timeout, false);
      }
   }
}
