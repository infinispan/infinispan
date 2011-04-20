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

import static java.util.Arrays.asList;

/**
 * A transaction logger to log ongoing transactions in an efficient and thread-safe manner while a rehash is going on.
 * <p/>
 * Transaction logs can then be replayed after the state transferred during a rehash has been written.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransactionLoggerImpl implements TransactionLogger {
   volatile boolean enabled;
   volatile Address writeLockOwner = null;
   final ReclosableLatch modsLatch = new ReclosableLatch();

   final BlockingQueue<WriteCommand> commandQueue = new LinkedBlockingQueue<WriteCommand>();
   final Map<GlobalTransaction, PrepareCommand> uncommittedPrepares = new ConcurrentHashMap<GlobalTransaction, PrepareCommand>();
   private static final Log log = LogFactory.getLog(TransactionLoggerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // A low number.  If we have less than this number of transactions, lock anyway.
   private static final int DRAIN_LOCK_THRESHOLD = 25;
   // If we see the queue growing this many times, lock.
   private static final int GROWTH_COUNT_THRESHOLD = 3;
   private int previousSize, growthCount;
   private final ReclosableLatch txBlockGate = new ReclosableLatch(true);


   private final CommandsFactory cf;

   public TransactionLoggerImpl(CommandsFactory cf) {
      this.cf = cf;
   }

   public void enable() {
      modsLatch.open();
      enabled = true;
   }

   public List<WriteCommand> drain() {
      List<WriteCommand> list = new LinkedList<WriteCommand>();
      commandQueue.drainTo(list);
      return list;
   }

   public List<WriteCommand> drainAndLock(Address lockedFor) {
      if (writeLockOwner != null) throw new IllegalStateException("This cannot happen - write lock already owned by " + writeLockOwner);

      modsLatch.close();
      if (writeLockOwner != null) throw new IllegalStateException("This cannot happen - write lock already owned by " + writeLockOwner);
      writeLockOwner = lockedFor;
      return drain();
   }

   public void unlockAndDisable(Address lockedFor) {
      boolean unlock = true;
      try {
         if (!lockedFor.equals(writeLockOwner)) {
            unlock = false;
            throw new IllegalMonitorStateException("Compare-and-set for owner " + lockedFor + " failed - was " + writeLockOwner);
         }

         enabled = false;
         uncommittedPrepares.clear();
         writeLockOwner = null;
      } catch (IllegalMonitorStateException imse) {
         log.unableToStopTransactionLogging(imse);
      } finally {
         if (unlock) modsLatch.open();
      }
   }

   public boolean logIfNeeded(WriteCommand command) {
      if (enabled) {
         waitForModsLatch();
         if (enabled) {
            try {
               commandQueue.put(command);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            return true;
         }
      }
      return false;
   }

   public void logIfNeeded(PrepareCommand command) {
      if (enabled) {
         waitForModsLatch();
         if (enabled) {
            if (command.isOnePhaseCommit())
               logModificationsInTransaction(command);
            else
               uncommittedPrepares.put(command.getGlobalTransaction(), command);
         }
      }
   }

   private void logModificationsInTransaction(PrepareCommand command) {
      logModifications(asList(command.getModifications()));
   }

   private void logModifications(Collection<WriteCommand> mods) {
      for (WriteCommand wc : mods) {
         try {
            commandQueue.put(wc);
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public void logModificationsIfNeeded(CommitCommand commit, TxInvocationContext context) {
      if (enabled) {
         waitForModsLatch();
         if (enabled) {
            GlobalTransaction gtx;
            if (!uncommittedPrepares.containsKey(gtx = commit.getGlobalTransaction()))
               uncommittedPrepares.put(gtx, cf.buildPrepareCommand(gtx, context.getModifications(), false));
         }
      }
   }

   public void logIfNeeded(CommitCommand command, TxInvocationContext context) {
      if (enabled) {
         waitForModsLatch();
         if (enabled) {
            PrepareCommand pc = uncommittedPrepares.remove(command.getGlobalTransaction());
            if (pc == null)
               logModifications(context.getModifications());
            else
               logModificationsInTransaction(pc);
         }
      }
   }

   public void logIfNeeded(RollbackCommand command) {
      if (enabled) {
         waitForModsLatch();
         if (enabled) uncommittedPrepares.remove(command.getGlobalTransaction());
      }
   }

   private void waitForModsLatch() {
      try {
         modsLatch.await();
      } catch (InterruptedException i) {
         Thread.currentThread().interrupt();
      }
   }

   private int size() {
      return enabled ? 0 : commandQueue.size();
   }

   public boolean isEnabled() {
      try {
         txBlockGate.await();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return enabled;
   }

   public boolean shouldDrainWithoutLock() {
      if (enabled) {
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

   public void blockNewTransactions() {
      txBlockGate.close();
   }

   public void unblockNewTransactions() {
      txBlockGate.open();
   }


   @Override
   public String toString() {
      return "TransactionLoggerImpl{" +
            "commandQueue=" + commandQueue +
            ", uncommittedPrepares=" + uncommittedPrepares +
            '}';
   }
}
