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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import java.util.concurrent.TimeoutException;

/**
 * Typically adding a command, the following pattern would be used:
 * <p/>
 *
 * <code>
 * if (stateTransferLock.acquireForCommand()) {
 *    try {
 *       // execute this command!
 *    } finally {
 *       stateTransferLock.releaseForCommand(cmd);
 *    }
 * }
 * </code>
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferLock {

   boolean acquireForCommand(InvocationContext ctx, WriteCommand command) throws InterruptedException, TimeoutException;

   boolean acquireForCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException;

   boolean acquireForCommand(TxInvocationContext ctx, CommitCommand command) throws InterruptedException, TimeoutException;

   boolean acquireForCommand(TxInvocationContext ctx, RollbackCommand command) throws InterruptedException, TimeoutException;

   boolean acquireForCommand(TxInvocationContext ctx, LockControlCommand cmd) throws TimeoutException, InterruptedException;

   void releaseForCommand(InvocationContext ctx, WriteCommand command);

   void releaseForCommand(TxInvocationContext ctx, PrepareCommand command);

   void releaseForCommand(TxInvocationContext ctx, CommitCommand command);

   void releaseForCommand(TxInvocationContext ctx, RollbackCommand command);

   void releaseForCommand(TxInvocationContext ctx, LockControlCommand cmd);


   void blockNewTransactions() throws InterruptedException;

   void unblockNewTransactions() throws InterruptedException;

   boolean areNewTransactionsBlocked();

   void waitForStateTransferToEnd(InvocationContext ctx, ReplicableCommand command) throws TimeoutException, InterruptedException;
}
