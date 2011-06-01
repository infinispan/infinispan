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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Typically adding a command, the following pattern would be used:
 * <p/>
 * <code>
 *
 * txLogger.beforeCommand();
 * try {
 *    // execute this command!
 * } finally {
 *    txLogger.afterCommand(cmd);
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
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.0
 */
public interface TransactionLogger extends RemoteTransactionLogger {
   /**
    * Enables transaction logging
    */
   void enable();

   /**
    * Logs a write command (if needed).
    *
    * @param command command to log
    */
   void afterCommand(WriteCommand command) throws InterruptedException;

   /**
    * Logs a PrepareCommand if needed.
    * @param command PrepoareCommand to log
    */
   void afterCommand(PrepareCommand command) throws InterruptedException;

   /**
    * Logs a CommitCommand if needed.
    * @param command CommitCommand to log
    */
   void afterCommand(CommitCommand command, TxInvocationContext context) throws InterruptedException;

   /**
    * Logs a RollbackCommand if needed.
    * @param command RollbackCommand to log
    */
   void afterCommand(RollbackCommand command) throws InterruptedException;

   /**
    * Checks whether transaction logging is enabled
    * @return true if enabled, false otherwise.
    */
   boolean isEnabled();

   /**
    * Notify the transaction logger before a write command, potentially blocking.
    */
   void beforeCommand(WriteCommand command) throws InterruptedException;

   /**
    * Notify the transaction logger before a prepare command, potentially blocking.
    */
   void beforeCommand(PrepareCommand command) throws InterruptedException;

   /**
    * Notify the transaction logger before a commit command, potentially blocking.
    * If transaction logging was not enabled during the prepare command, use the
    * context to extract the list of modifications.
    */
   void beforeCommand(CommitCommand command, TxInvocationContext context) throws InterruptedException;

   /**
    * Notify the transaction logger before a rollback command, potentially blocking.
    */
   void beforeCommand(RollbackCommand command) throws InterruptedException;

   /**
    * Causes new transactions to block when calling <code>beforeCommand()</code>.
    */
   void blockNewTransactions() throws InterruptedException;

   /**
    * Unblocks anything blocking on <code>beforeCommand()</code>.
    */
   void unblockNewTransactions() throws InterruptedException;
}
