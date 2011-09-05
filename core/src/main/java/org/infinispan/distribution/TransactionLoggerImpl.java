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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Arrays.asList;

/**
 * A transaction logger to log ongoing transactions in an efficient and thread-safe manner while a rehash is going on.
 * <p/>
 * Transaction logs can then be replayed after the state transferred during a rehash has been written.
 *
 * @author Manik Surtani
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.0
 */
public class TransactionLoggerImpl implements TransactionLogger {
   private static final Log log = LogFactory.getLog(TransactionLoggerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // A low number.  If we have less than this number of transactions, lock anyway.
   private static final int DRAIN_LOCK_THRESHOLD = 25;
   // If we see the queue growing this many times, lock.
   private static final int GROWTH_COUNT_THRESHOLD = 3;
   private int previousSize, growthCount;

   private volatile boolean loggingEnabled;
   // This lock is used to block new transactions during rehash
   // Write commands must acquire the read lock for the duration of the command
   // We acquire the write lock to block new transactions
   // That means we wait for pending write commands to finish, and we might have to wait a lot if
   // a command is deadlocked
   // TODO Find a way to interrupt all transactions waiting for answers from remote nodes, instead
   // of waiting for all of them to finish
   private ReentrantReadWriteLock txLock = new ReentrantReadWriteLock();
   private ReclosableLatch txLockLatch = new ReclosableLatch(true);

   final BlockingQueue<WriteCommand> commandQueue = new LinkedBlockingQueue<WriteCommand>();
   final Map<GlobalTransaction, PrepareCommand> uncommittedPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>();

   private final CommandsFactory cf;
   private long lockTimeout;

   public TransactionLoggerImpl(CommandsFactory cf, Configuration config) {
      this.cf = cf;
      this.lockTimeout = config.getRehashWaitTime();
   }

   @Override
   public void enable() {
      loggingEnabled = true;
   }

   @Override
   public List<WriteCommand> drain() {
      List<WriteCommand> list = new ArrayList<WriteCommand>();
      commandQueue.drainTo(list);
      return list;
   }

   @Override
   public List<WriteCommand> drainAndLock() throws InterruptedException {
      blockNewTransactions();
      return drain();
   }

   @Override
   public void unlockAndDisable() {
      loggingEnabled = false;
      uncommittedPrepares.clear();
      unblockNewTransactions();
   }

   @Override
   public void afterCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException {
      // for transactions the real work starts with the prepare command, so don't log anything here
      if (ctx.isInTxScope())
         return;

      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();

      if (loggingEnabled && command.isSuccessful()) {
         commandQueue.put(command);
      }
   }

   @Override
   public void afterCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();

      if (loggingEnabled) {
         if (command.isOnePhaseCommit())
            logModificationsInTransaction(command);
         else
            uncommittedPrepares.put(command.getGlobalTransaction(), command);
      }
   }

   @Override
   public void afterCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
         releaseLockForTx();

      if (loggingEnabled) {
         PrepareCommand pc = uncommittedPrepares.remove(command.getGlobalTransaction());
         if (pc == null)
            logModifications(ctx.getModifications());
         else
            logModificationsInTransaction(pc);
      }
   }

   @Override
   public void afterCommand(TxInvocationContext ctx, RollbackCommand command) {
      // don't need to lock, rollbacks won't touch the data container

      if (loggingEnabled) {
         uncommittedPrepares.remove(command.getGlobalTransaction());
      }
   }

   @Override
   public void afterCommand(TxInvocationContext ctx, LockControlCommand command) {
      if (!command.isUnlock())
         releaseLockForTx();
   }

   private void logModificationsInTransaction(PrepareCommand command) throws InterruptedException {
      logModifications(asList(command.getModifications()));
   }

   private void logModifications(Collection<WriteCommand> mods) throws InterruptedException {
      for (WriteCommand wc : mods) {
         commandQueue.put(wc);
      }
   }

   @Override
   public boolean beforeCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException, TimeoutException {
      // for transactions the real work starts with the prepare command, so don't block here
      if (ctx.isInTxScope() || ctx.hasFlag(Flag.SKIP_LOCKING))
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public boolean beforeCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException {
      if (ctx.hasFlag(Flag.SKIP_LOCKING))
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public boolean beforeCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         if (!acquireLockForTx(ctx))
            return false;
      }

      // if the prepare command wasn't logged, do it here instead
      if (loggingEnabled) {
         GlobalTransaction gtx;
         if (!uncommittedPrepares.containsKey(gtx = ctx.getGlobalTransaction()))
            uncommittedPrepares.put(gtx, cf.buildPrepareCommand(gtx, ctx.getModifications(), false));
      }

      return true;
   }

   @Override
   public boolean beforeCommand(TxInvocationContext ctx, RollbackCommand command) throws InterruptedException, TimeoutException {
      // do nothing, rollbacks won't touch the data container
      return true;
   }

   @Override
   public boolean beforeCommand(TxInvocationContext ctx, LockControlCommand cmd) throws TimeoutException, InterruptedException {
      if (cmd.isUnlock())
         return true;

      return acquireLockForTx(ctx);
   }

   @Override
   public void suspendTransactionLock(InvocationContext ctx) {
      releaseLockForTx();
   }

   @Override
   public void resumeTransactionLock(InvocationContext ctx) throws TimeoutException, InterruptedException {
      acquireLockForTx(ctx);
   }

   private int size() {
      return loggingEnabled ? commandQueue.size() : 0;
   }

   @Override
   public boolean isEnabled() {
      return loggingEnabled;
   }

   @Override
   public boolean shouldDrainWithoutLock() {
      if (loggingEnabled) {
         int sz = size();
         boolean shouldLock = (previousSize > 0 && growthCount > GROWTH_COUNT_THRESHOLD) || sz < DRAIN_LOCK_THRESHOLD;
         if (!shouldLock) {
            if (sz > previousSize && previousSize > 0) growthCount++;
            previousSize = sz;
            return true;
         } else {
            return false;
         }
      } else return false;
   }

   @Override
   public Collection<PrepareCommand> getPendingPrepares() {
      Collection<PrepareCommand> commands = new HashSet<PrepareCommand>(uncommittedPrepares.values());
      uncommittedPrepares.clear();
      return commands;
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
   public boolean areTransactionsBlocked() {
      try {
         return txLock.readLock().tryLock(0, TimeUnit.SECONDS);
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

   private void releaseLockForTx() {
      txLock.readLock().unlock();
   }


   @Override
   public String toString() {
      return "TransactionLoggerImpl{" +
            "commandQueue=" + commandQueue +
            ", uncommittedPrepares=" + uncommittedPrepares +
            '}';
   }
}
