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

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A transaction logger to log ongoing transactions in an efficient and thread-safe manner while a rehash is going on.
 * <p/>
 * Transaction logs can then be replayed after the state transferred during a rehash has been written.
 *
 * @author Manik Surtani
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.0
 */
public class StateTransferLockImpl implements StateTransferLock {
   private static final Log log = LogFactory.getLog(StateTransferLockImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // This lock is used to block new transactions during rehash
   // Write commands must acquire the read lock for the duration of the command
   // We acquire the write lock to block new transactions
   // That means we wait for pending write commands to finish, and we might have to wait a lot if
   // a command is deadlocked
   // TODO Find a way to interrupt all transactions waiting for answers from remote nodes, instead
   // of waiting for all of them to finish
   // Could also suspend the tx lock during any key lock operation > 1s (configurable)
   // but we would need to retry the whole command after the consistent hash has been changed
   private ReentrantReadWriteLock txLock = new ReentrantReadWriteLock();
   private ReclosableLatch txLockLatch = new ReclosableLatch(true);

   private long lockTimeout;
   private boolean eagerLockingEnabled;

   public StateTransferLockImpl() {
   }

   @Inject
   public void injectDependencies(Configuration config) {
      this.lockTimeout = config.getRehashWaitTime();
   }

   @Override
   public void releaseForCommand(InvocationContext ctx, WriteCommand command) {
      // for transactions the real work starts with the prepare command, so don't log anything here
      if (ctx.isInTxScope() && !eagerLockingEnabled)
         return;

      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, CommitCommand command) {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, RollbackCommand command) {
      // don't need to lock, rollbacks won't touch the data container
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, LockControlCommand command) {
      if (!command.isUnlock())
         releaseLockForTx();
   }

   @Override
   public boolean acquireForCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException, TimeoutException {
      // for transactions the real work starts with the prepare command, so don't block here
      if ((ctx.isInTxScope() && !eagerLockingEnabled) || ctx.hasFlag(Flag.SKIP_LOCKING))
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException {
      if (ctx.hasFlag(Flag.SKIP_LOCKING))
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         if (!acquireLockForTx(ctx))
            return false;
      }
      return true;
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, RollbackCommand command) throws InterruptedException, TimeoutException {
      // do nothing, rollbacks won't touch the data container
      return true;
   }

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, LockControlCommand cmd) throws TimeoutException, InterruptedException {
      if (cmd.isUnlock())
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public void waitForStateTransferToEnd(InvocationContext ctx, ReplicableCommand cmd) throws TimeoutException, InterruptedException {
      if (areNewTransactionsBlocked()) {
         if (releaseLockForTx()) {
            acquireLockForTx(ctx);
         }
      }
   }

   @Override
   public void blockNewTransactions() throws InterruptedException {
      if (!txLock.isWriteLockedByCurrentThread()) {
         if (trace) log.debug("Blocking new transactions");
         txLockLatch.close();
         // we want to ensure that all the modifications that passed through the tx gate have ended
         txLock.writeLock().lockInterruptibly();
      } else {
         if (trace) log.debug("New transactions were not unblocked by the previous rehash");
      }
   }

   @Override
   public void unblockNewTransactions() {
      if (trace) log.debug("Unblocking new transactions");
      txLock.writeLock().unlock();
      // only for lock commands
      txLockLatch.open();
   }

   @Override
   public boolean areNewTransactionsBlocked() {
      try {
         return !txLockLatch.await(0, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return false;
      }
   }

   private boolean acquireLockForTx(InvocationContext ctx) throws InterruptedException, TimeoutException {
      // hold the read lock to ensure the rehash process waits for the tx to end
      // first try with 0 timeout, in case a rehash is not in progress
      if (txLockLatch.await(0, TimeUnit.MILLISECONDS)) {
         if (txLock.readLock().tryLock(0, TimeUnit.MILLISECONDS))
            return true;
      }

      // When the command is being replicated, the caller already holds the tx lock for read on the
      // origin since DistributionInterceptor is above DistTxInterceptor in the interceptor chain.
      // In order to allow the rehashing thread on the origin to obtain the tx lock for write on the
      // origin, we only lock on the remote nodes with 0 timeout.
      if (!ctx.isOriginLocal())
         return false;

      // A rehash may be in progress, wait for it to end
      // But another transaction may have obtained the tx lock and be waiting on one of the keys locked by us
      // So if we have any locks wait for a much shorter amount of time
      // We do a separate wait here because we don't want to call ctx.getLockedKeys() all the time
      boolean hasAcquiredLocks = ctx.getLockedKeys().size() > 0;
      long timeout = hasAcquiredLocks ? lockTimeout / 100 : lockTimeout;
      long endTime = System.currentTimeMillis() + timeout;
      while (true) {
         // first check the latch
         if (!txLockLatch.await(timeout, TimeUnit.MILLISECONDS))
            return false;

         // hold the read lock to ensure the rehash process waits for the tx to end
         if (txLock.readLock().tryLock(0, TimeUnit.MILLISECONDS))
            return true;

         // the rehashing thread has acquired the write lock between our latch check and our read lock attempt
         // retry, unless the timeout expired
         timeout = endTime - System.currentTimeMillis();
         if (timeout < 0)
            return false;
      }
   }

   private boolean releaseLockForTx() {
      int holdCount = txLock.getReadHoldCount();
      if (holdCount > 1)
         throw new IllegalStateException("Transaction lock should not be acquired more than once by any thread");
      if (holdCount == 1) {
         txLock.readLock().unlock();
         return true;
      } else {
         log.trace("Transaction lock was not previously previously acquired by this thread, not releasing");
         return false;
      }
   }


   @Override
   public String toString() {
      return "TransactionLoggerImpl{" +
            "transactions blocked=" + areNewTransactionsBlocked() +
            '}';
   }
}
