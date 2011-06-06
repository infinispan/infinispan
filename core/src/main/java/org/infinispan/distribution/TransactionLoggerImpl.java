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
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
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

   final BlockingQueue<WriteCommand> commandQueue = new LinkedBlockingQueue<WriteCommand>();
   final Map<GlobalTransaction, PrepareCommand> uncommittedPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>();

   private final CommandsFactory cf;

   public TransactionLoggerImpl(CommandsFactory cf) {
      this.cf = cf;
   }

   public void enable() {
      loggingEnabled = true;
   }

   public List<WriteCommand> drain() {
      List<WriteCommand> list = new LinkedList<WriteCommand>();
      commandQueue.drainTo(list);
      return list;
   }

   public List<WriteCommand> drainAndLock() throws InterruptedException {
      blockNewTransactions();
      return drain();
   }

   public void unlockAndDisable() {
      loggingEnabled = false;
      uncommittedPrepares.clear();
      unblockNewTransactions();
   }

   public void afterCommand(WriteCommand command) throws InterruptedException {
      txLock.readLock().unlock();
      if (loggingEnabled && command.isSuccessful()) {
         commandQueue.put(command);
      }
   }

   public void afterCommand(PrepareCommand command) throws InterruptedException {
      txLock.readLock().unlock();
      if (loggingEnabled) {
         if (command.isOnePhaseCommit())
            logModificationsInTransaction(command);
         else
            uncommittedPrepares.put(command.getGlobalTransaction(), command);
      }
   }

   public void afterCommand(CommitCommand command, TxInvocationContext context) throws InterruptedException {
      txLock.readLock().unlock();
      if (loggingEnabled) {
         PrepareCommand pc = uncommittedPrepares.remove(command.getGlobalTransaction());
         if (pc == null)
            logModifications(context.getModifications());
         else
            logModificationsInTransaction(pc);
      }
   }

   public void afterCommand(RollbackCommand command) {
      txLock.readLock().unlock();
      if (loggingEnabled) {
         uncommittedPrepares.remove(command.getGlobalTransaction());
      }
   }

   private void logModificationsInTransaction(PrepareCommand command) throws InterruptedException {
      logModifications(asList(command.getModifications()));
   }

   private void logModifications(Collection<WriteCommand> mods) throws InterruptedException {
      for (WriteCommand wc : mods) {
         commandQueue.put(wc);
      }
   }

   public void beforeCommand(WriteCommand command) throws InterruptedException {
      txLock.readLock().lock();
   }

   public void beforeCommand(PrepareCommand command) throws InterruptedException {
      txLock.readLock().lock();
   }

   public void beforeCommand(CommitCommand command, TxInvocationContext context) throws InterruptedException {
      txLock.readLock().lock();

      // if the prepare command wasn't logged, do it here instead
      if (loggingEnabled) {
         GlobalTransaction gtx;
         if (!uncommittedPrepares.containsKey(gtx = context.getGlobalTransaction()))
            uncommittedPrepares.put(gtx, cf.buildPrepareCommand(gtx, context.getModifications(), false));
      }
   }

   public void beforeCommand(RollbackCommand command) throws InterruptedException {
      txLock.readLock().lock();
   }

   private int size() {
      return loggingEnabled ? commandQueue.size() : 0;
   }

   public boolean isEnabled() {
      return loggingEnabled;
   }

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

   public Collection<PrepareCommand> getPendingPrepares() {
      Collection<PrepareCommand> commands = new HashSet<PrepareCommand>(uncommittedPrepares.values());
      uncommittedPrepares.clear();
      return commands;
   }

   public void blockNewTransactions() throws InterruptedException {
      if (trace) log.debug("Blocking new transactions");
      // we just want to ensure that all the modifications that passed through the tx gate have ended
      txLock.writeLock().lockInterruptibly();
   }

   public void unblockNewTransactions() {
      if (trace) log.debug("Unblocking new transactions");
      txLock.writeLock().unlock();
   }


   @Override
   public String toString() {
      return "TransactionLoggerImpl{" +
            "commandQueue=" + commandQueue +
            ", uncommittedPrepares=" + uncommittedPrepares +
            '}';
   }
}
