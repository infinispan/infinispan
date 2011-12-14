/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.statetransfer;

import org.infinispan.CacheException;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements a specialized lock that allows the state transfer process (which is not a single thread)
 * to block new write commands for the duration of the state transfer.
 * <p/>
 * At the same time the block call will not return until any running write commands have finished executing.
 * <p/>
 * Write commands in a transaction scope don't actually write anything, so they are ignored. Lock commands on
 * the other hand, both explicit and implicit, are considered as write commands for the purpose of this lock.
 * <p/>
 * Commit commands, rollback commands and unlock commands are special in that letting them proceed may speed up other
 * running commands, so they are allowed to proceed as long as there are any running write commands. Commit is also
 * a write command, so the block call will wait until all commit commands have finished.
 *
 * @author Manik Surtani
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public class StateTransferLockImpl implements StateTransferLock {
   private static final Log log = LogFactory.getLog(StateTransferLockImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // TODO Find a way to interrupt all transactions waiting for answers from remote nodes or waiting on key locks
   // TODO Reuse the ReentrantReadWriteLock's Sync and put all the state in one volatile
   private AtomicInteger runningWritesCount = new AtomicInteger(0);
   private volatile boolean writesShouldBlock;
   private volatile boolean writesBlocked;
   private ThreadLocal<Boolean> traceThreadWrites = new ThreadLocal<Boolean>();
   private int blockingCacheViewId = NO_BLOCKING_CACHE_VIEW;
   // blockingCacheViewId, writesShouldBlock and writesBlocked should only be modified while holding lock and always in this order
   private final Object lock = new Object();

   // stored configuration options
   private boolean stateTransferEnabled;
   private boolean pessimisticLocking;
   private long lockTimeout;

   public StateTransferLockImpl() {
   }

   @Inject
   public void injectDependencies(Configuration config) {
      stateTransferEnabled = (config.getCacheMode().isDistributed() && config.isRehashEnabled())
            || (config.getCacheMode().isReplicated() && config.isStateTransferEnabled());
      pessimisticLocking =  config.getTransactionLockingMode() == LockingMode.PESSIMISTIC;
      lockTimeout = config.getRehashWaitTime();
   }

   @Override
   public void releaseForCommand(InvocationContext ctx, WriteCommand command) {
      if (shouldAcquireLock(ctx, command))
         releaseLockForWrite();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (shouldAcquireLock(ctx, command))
         releaseLockForWrite();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, CommitCommand command) {
      if (shouldAcquireLock(ctx, command))
         releaseLockForWrite();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, RollbackCommand command) {
      // don't need to lock, rollbacks won't touch the data container
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, LockControlCommand command) {
      if (shouldAcquireLock(ctx, command))
         releaseLockForWrite();
   }

   @Override
   public boolean acquireForCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException, TimeoutException {
      if (!shouldAcquireLock(ctx, command))
         return true;

      return acquireLockForWriteCommand(ctx);
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException {
      if (!shouldAcquireLock(ctx, command))
         return true;

      return acquireLockForWriteCommand(ctx);
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException, TimeoutException {
      if (!shouldAcquireLock(ctx, command))
         return true;

      return acquireLockForCommitCommand(ctx);
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, RollbackCommand command) throws InterruptedException, TimeoutException {
      // do nothing, rollbacks won't touch the data container
      return true;
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, LockControlCommand command) throws TimeoutException, InterruptedException {
      if (!shouldAcquireLock(ctx, command))
         return true;

      return acquireLockForWriteCommand(ctx);
   }


   private boolean shouldAcquireLock(InvocationContext ctx, WriteCommand command) {
      // For transactions with optimistic locking the real work starts with the prepare command, so don't block here.
      // With pessimistic locking an implicit lock command is created, but the invocation skips some interceptors
      // so we need to block for write commands as well.
      return !(ctx.isInTxScope() && !pessimisticLocking) && !ctx.hasFlag(Flag.SKIP_LOCKING);
   }

   private boolean shouldAcquireLock(TxInvocationContext ctx, PrepareCommand command) {
      return !ctx.hasFlag(Flag.SKIP_LOCKING);
   }

   private boolean shouldAcquireLock(TxInvocationContext ctx, CommitCommand command) {
      return !ctx.hasFlag(Flag.SKIP_LOCKING);
   }

   private boolean shouldAcquireLock(TxInvocationContext ctx, RollbackCommand command) {
      return false;
   }

   private boolean shouldAcquireLock(TxInvocationContext ctx, LockControlCommand command) {
      return !command.isUnlock();
   }


   @Override
   public void waitForStateTransferToEnd(InvocationContext ctx, VisitableCommand command, int newCacheViewId) throws TimeoutException, InterruptedException {
      // in the most common case there we don't know anything about a state transfer in progress so we return immediately
      // it's ok to access blockingCacheViewId without a lock here, in the worst case scenario we do the lock below
      if (!writesShouldBlock && newCacheViewId <= blockingCacheViewId)
         return;

      boolean shouldSuspendLock;
      try {
         shouldSuspendLock = (Boolean)command.acceptVisitor(ctx, new ShouldAcquireLockVisitor());
      } catch (Throwable throwable) {
         throw new CacheException("Unexpected exception", throwable);
      }

      if (shouldSuspendLock) {
         log.tracef("Suspending shared state transfer lock to allow state transfer to start (and end)");
         releaseLockForWrite();

         // we got a newer cache view id from a remote node, so we know it will be installed on this node as well
         // even if the cache view installation is cancelled, the rollback will advance the view id so we won't wait forever
         if (blockingCacheViewId < newCacheViewId) {
            long end = System.currentTimeMillis() + lockTimeout;
            long timeout = lockTimeout;
            synchronized (lock) {
               while (timeout >= 0 && blockingCacheViewId < newCacheViewId) {
                  if (trace) log.tracef("We are waiting for cache view %d, right now we have %d", newCacheViewId, blockingCacheViewId);
                  lock.wait(timeout);
                  timeout = end - System.currentTimeMillis();
               }
            }
         }

         acquireLockForWriteCommand(ctx);
      }
   }

   @Override
   public void blockNewTransactions(int cacheViewId) throws InterruptedException {
      log.debugf("Blocking new write commands for cache view %d", cacheViewId);

      synchronized (lock) {
         writesShouldBlock = true;
         if (writesBlocked == true) {
            if (blockingCacheViewId < cacheViewId) {
               log.tracef("Write commands were already blocked for cache view %d", blockingCacheViewId);
            } else {
               throw new IllegalStateException(String.format("Trying to block write commands but they are already blocked for view %d", blockingCacheViewId));
            }
         }

         // TODO Add a timeout parameter
         while (runningWritesCount.get() != 0) {
            lock.wait();
         }
         writesBlocked = true;
         blockingCacheViewId = cacheViewId;
      }
      log.tracef("New write commands blocked");
   }

   @Override
   public void blockNewTransactionsAsync() {
      if (!writesShouldBlock) {
         log.debugf("Blocking new write commands because we'll soon start a state transfer");
         writesShouldBlock = true;
      }
   }

   @Override
   public void unblockNewTransactions(int cacheViewId) {
      log.debugf("Unblocking write commands for cache view %d", cacheViewId);
      synchronized (lock) {
         if (!writesBlocked)
            throw new IllegalStateException(String.format("Trying to unblock write commands for cache view %d but they were not blocked", cacheViewId));
         writesShouldBlock = false;
         writesBlocked = false;
         lock.notifyAll();

         // throw the view id mismatch exception only after we have released the lock
         // so that a future state transfer will be able to proceed normally
         if (cacheViewId != blockingCacheViewId && blockingCacheViewId != NO_BLOCKING_CACHE_VIEW)
            throw new IllegalStateException(String.format("Trying to unblock write commands for cache view %d, but they were blocked with view id %d",
                  cacheViewId, blockingCacheViewId));
      }
      log.tracef("Unblocked write commands for cache view %d", cacheViewId);
   }

   @Override
   public boolean areNewTransactionsBlocked() {
      return writesShouldBlock;
   }

   @Override
   public int getBlockingCacheViewId() {
      return blockingCacheViewId;
   }

   private boolean acquireLockForWriteCommand(InvocationContext ctx) throws InterruptedException, TimeoutException {
      // first we handle the fast path, when writes are not blocked
      if (acquireLockForWriteNoWait()) return true;

      // When the command is being replicated, the caller already holds the tx lock for read on the
      // origin since DistributionInterceptor is above DistTxInterceptor in the interceptor chain.
      // In order to allow the rehashing thread on the origin to obtain the tx lock for write on the
      // origin, we never wait for the state transfer lock on remote nodes.
      // The originator should wait for the state transfer to end and retry the command
      if (!ctx.isOriginLocal())
         return false;

      // A state transfer is in progress, wait for it to end
      long timeout = lockTimeout;
      long endTime = System.currentTimeMillis() + lockTimeout;
      synchronized (lock) {
         while (true) {
            // wait for the unblocker thread to notify us
            lock.wait(timeout);

            if (acquireLockForWriteNoWait())
               return true;

            // retry, unless the timeout expired
            timeout = endTime - System.currentTimeMillis();
            if (timeout < 0)
               return false;
         }
      }
   }

   private boolean acquireLockForWriteNoWait() {
      // Because we use multiple volatile variables for the state this involves a lot of volatile reads
      // (at least 1 read of writesShouldBlock, 1 read+write of runningWritesCount)
      // With one state variable the fast path should go down to 1 read + 1 cas
      if (!writesShouldBlock) {
         int previousWrites = runningWritesCount.getAndIncrement();
         // if there were no other write commands running someone could have blocked new writes
         // check the local first to skip a volatile read on writesShouldBlock
         if (previousWrites != 0 || !writesShouldBlock) {
            if (trace) {
               if (traceThreadWrites.get() == Boolean.TRUE)
                  log.error("Trying to acquire state transfer shared lock, but this thread already has it", new Exception());
               traceThreadWrites.set(Boolean.TRUE);
               log.tracef("Acquired shared state transfer shared lock, total holders: %d", runningWritesCount.get());
            }
            return true;
         }

         // roll back the runningWritesCount, we didn't get the lock
         runningWritesCount.decrementAndGet();
      }
      return false;
   }

   // Duplicated acquireLockForWriteCommand to allow commits while writesShouldBlock == true but writesBlocked == false
   private boolean acquireLockForCommitCommand(InvocationContext ctx) throws InterruptedException, TimeoutException {
      // first we handle the fast path, when writes are not blocked
      if (acquireLockForCommitNoWait()) return true;

      // When the command is being replicated, the caller already holds the tx lock for read on the
      // origin since DistributionInterceptor is above DistTxInterceptor in the interceptor chain.
      // In order to allow the rehashing thread on the origin to obtain the tx lock for write on the
      // origin, we never wait for the state transfer lock on remote nodes.
      // The originator should wait for the state transfer to end and retry the command
      if (!ctx.isOriginLocal())
         return false;

      // A state transfer is in progress, wait for it to end
      // A commit command should never fail on the originator, so wait forever
      synchronized (lock) {
         while (true) {
            // wait for the unblocker thread to notify us
            lock.wait();

            if (acquireLockForCommitNoWait())
               return true;
         }
      }
   }

   private boolean acquireLockForCommitNoWait() {
      // Because we use multiple volatile for the state this involves a lot of volatile reads
      // (at least 1 read of writesShouldBlock, 1 read+write of runningWritesCount)
      if (!writesBlocked) {
         int previousWrites = runningWritesCount.getAndIncrement();
         // if there were no other write commands running someone could have blocked new writes
         // check the local first to skip a volatile read on writesBlocked
         if (previousWrites != 0 || !writesBlocked) {
            if (trace) {
               if (traceThreadWrites.get() == Boolean.TRUE)
                  log.error("Trying to acquire state transfer shared lock, but this thread already has it", new Exception());
               traceThreadWrites.set(Boolean.TRUE);
               log.tracef("Acquired shared state transfer shared lock (for commit), total holders: %d", runningWritesCount.get());
            }
            return true;
         }

         // roll back the runningWritesCount, we didn't get the lock
         runningWritesCount.decrementAndGet();
      }
      return false;
   }

   private void releaseLockForWrite() {
      if (trace) {
         if (traceThreadWrites.get() != Boolean.TRUE)
            log.error("Trying to release state transfer shared lock without acquiring it first", new Exception());
         traceThreadWrites.set(null);
      }
      int remainingWrites = runningWritesCount.decrementAndGet();
      if (remainingWrites < 0) {
         throw new IllegalStateException("Trying to release state transfer shared lock without acquiring it first");
      } else if (remainingWrites == 0) {
         synchronized (lock) {
            lock.notifyAll();
         }
      }

      if (trace) log.tracef("Released shared state transfer shared lock, remaining holders: %d", remainingWrites);
   }


   @Override
   public String toString() {
      return "StateTransferLockImpl{" +
            "runningWritesCount=" + runningWritesCount +
            ", writesShouldBlock=" + writesShouldBlock +
            ", writesBlocked=" + writesBlocked +
            ", blockingCacheViewId=" + blockingCacheViewId +
            '}';
   }

   private class ShouldAcquireLockVisitor extends AbstractVisitor {
      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         return shouldAcquireLock(ctx, command);
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         return Boolean.FALSE;
      }
   }
}
