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
package org.infinispan.interceptors;

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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.StateTransferInProgressException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * An interceptor that any commands when the {@link StateTransferLock} is locked.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public class StateTransferLockInterceptor extends CommandInterceptor {

   public static final int RETRY_COUNT = 3;
   StateTransferLock stateTransferLock;

   @Inject
   public void injectDistributionManager(StateTransferLock stateTransferLock) {
      this.stateTransferLock = stateTransferLock;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         // TODO If the super call throws a StateTransferInProgressException, we should release the state transfer lock
         // to allow any pending rehash to finish and then retry the operation
         // Then we could throw a StateTransferInProgressException only on remote nodes and include the view id in the
         // exception message to make sure we got the right rehash
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPrepareCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return super.visitRollbackCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new StateTransferInProgressException(stateTransferLock.getBlockingCacheViewId(), "Timed out waiting for the transaction lock");
      }
      try {
         return handleWithRetries(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   private Object handleWithRetries(InvocationContext ctx, VisitableCommand command) throws Throwable {
      int retries = RETRY_COUNT;
      while (true) {
         int newCacheViewId = -1;
         try {
            return invokeNextInterceptor(ctx, command);
         } catch (StateTransferInProgressException e) {
            newCacheViewId = e.getNewCacheViewId();
            if (retries < 0) {
               throw new TimeoutException("Timed out waiting for the state transfer to end", e);
            }
         } catch (SuspectException e) {
            if (retries < 0) {
               throw new TimeoutException("Timed out waiting for the state transfer to end", e);
            }
         }

         // the remote node has thrown an exception, but we will retry the operation
         // we are assuming the current node is also trying to start the rehash, but it can't
         // because we're holding the tx lock
         // so we release our state transfer lock temporarily to allow the state transfer to end
         // after that we can retry our command
         stateTransferLock.waitForStateTransferToEnd(ctx, command, newCacheViewId);
      }
   }

}
