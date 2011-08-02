/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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

import java.util.*;
import java.util.concurrent.*;
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
      this.lockTimeout = config.getLockAcquisitionTimeout();
   }

   @Override
   public void enable() {
      loggingEnabled = true;
   }

   @Override
   public List<WriteCommand> drain() {
      List<WriteCommand> list = new LinkedList<WriteCommand>();
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
//      if (!ctx.hasFlag(Flag.SKIP_LOCKING))
//         releaseLockForTx();

      if (loggingEnabled) {
         uncommittedPrepares.remove(command.getGlobalTransaction());
      }
   }

   @Override
   public void afterCommand(TxInvocationContext ctx, LockControlCommand command) {
      if (!command.isUnlock())
         releaseLockForTx();
      // TODO Log lock control commands too
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
   public void beforeCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException, TimeoutException {
      // for transactions the real work starts with the prepare command, so don't block here
      if (ctx.isInTxScope())
         return;

      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         acquireLockForTx(ctx);
      }
   }

   @Override
   public void beforeCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         acquireLockForTx(ctx);
      }
   }

   @Override
   public void beforeCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         acquireLockForTx(ctx);
      }

      // if the prepare command wasn't logged, do it here instead
      if (loggingEnabled) {
         GlobalTransaction gtx;
         if (!uncommittedPrepares.containsKey(gtx = ctx.getGlobalTransaction()))
            uncommittedPrepares.put(gtx, cf.buildPrepareCommand(gtx, ctx.getModifications(), false));
      }
   }

   @Override
   public void beforeCommand(TxInvocationContext ctx, RollbackCommand command) throws InterruptedException, TimeoutException {
//      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
//         acquireLockForTx(ctx);
//      }
   }

   @Override
   public void beforeCommand(TxInvocationContext ctx, LockControlCommand cmd) throws TimeoutException, InterruptedException {
      if (!cmd.isUnlock()) {
         acquireLockForTx(ctx);
      }
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
      if (trace) log.debug("Blocking new transactions");
      txLockLatch.close();
      // we want to ensure that all the modifications that passed through the tx gate have ended
      txLock.writeLock().lockInterruptibly();
   }

   @Override
   public void unblockNewTransactions() {
      if (trace) log.debug("Unblocking new transactions");
      txLock.writeLock().unlock();
      txLockLatch.open();
   }


   private void acquireLockForTx(InvocationContext ctx) throws InterruptedException, TimeoutException {
      // When the command is being replicated, the caller already holds the tx lock for read on the
      // origin since DistributionInterceptor is above DistTxInterceptor in the interceptor chain.
      // In order to allow the rehashing thread on the origin to obtain the tx lock for write on the
      // origin, we lock on the remote nodes with 0 timeout.
      // TODO With numOwners > 2 there is still a chance that locking will succeed on some of the remote nodes
      // which could be a problem if the command is a CommitCommand
      long timeout = ctx.isOriginLocal() ? lockTimeout : 0;
      if (!txLockLatch.await(timeout, TimeUnit.MILLISECONDS))
         throw new TimeoutException("Timed out waiting for the transaction lock to be released after " + timeout + "ms");
      // hold the read lock to ensure the rehash process waits for the tx to end
      if (!txLock.readLock().tryLock())
         throw new IllegalStateException("Tx lock should not be locked for writing as long as the latch is open");
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
