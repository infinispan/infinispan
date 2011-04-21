/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Typically adding a command, the following pattern would be used:
 * <p/>
 * <code>
 *
 * if (txLogger.logIfNeeded(cmd)) {
 *     // do NOT proceed with executing this command!
 * } else {
 *     // proceed with executing this command as per normal!
 * }
 *
 * </code>
 * <p/>
 * When draining, the following pattern should be used:
 * <p/>
 * <code>
 *
 * List&lt;WriteCommand&gt; c = null;
 * while (txLogger.shouldDrainWithoutLock()) {
 *     c = txLogger.drain();
 *     applyCommands(c);
 * }
 *
 * c = txLogger.drainAndLock();
 * applyCommands(c);
 * applyPendingPrepares(txLogger.getPendingPrepares());
 * txLogger.unlockAndDisable();
 * </code>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface TransactionLogger extends RemoteTransactionLogger {
   /**
    * Enables transaction logging
    */
   void enable();

   /**
    * If logging is enabled, will log the command and return true.  Otherwise, will just return false.
    *
    * @param command command to log
    * @return true if logged, false otherwise
    */
   boolean logIfNeeded(WriteCommand command);

   /**
    * Logs a PrepareCommand if needed.
    * @param command PrepoareCommand to log
    */
   void logIfNeeded(PrepareCommand command);

   /**
    * Logs a CommitCommand if needed.
    * @param command CommitCommand to log
    */
   void logIfNeeded(CommitCommand command, TxInvocationContext context);

   /**
    * Logs a RollbackCommand if needed.
    * @param command RollbackCommand to log
    */
   void logIfNeeded(RollbackCommand command);

   /**
    * Checks whether transaction logging is enabled
    * @return true if enabled, false otherwise.
    */
   boolean isEnabled();

   /**
    * A mechanism for commit commands to register modifications instead of a prepare.  Used for when transaction logging
    * was disabled during prepare, but was enabled before commit.
    * @param commit commit command
    * @param context context from which to extract modification list
    */
   void logModificationsIfNeeded(CommitCommand commit, TxInvocationContext context);

   /**
    * Causes new transactions to block when testing isEnabled().
    */
   void blockNewTransactions();

   /**
    * Unblocks anything blocking on isEnabled().
    */
   void unblockNewTransactions();
}
