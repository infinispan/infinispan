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
import org.infinispan.distribution.RehashInProgressException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.statetransfer.StateTransferLock;

/**
 * An interceptor that any commands when the {@link StateTransferLock} is locked.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public class StateTransferLockInterceptor extends CommandInterceptor {

   StateTransferLock stateTransferLock;

   @Inject
   public void injectDistributionManager(StateTransferLock stateTransferLock) {
      this.stateTransferLock = stateTransferLock;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, cmd)) {
         // TODO If the super call throws a RehashInProgressException, we should release the state transfer lock
         // to allow any pending rehash to finish and then retry the operation
         // Then we could throw a RehashInProgressException only on remote nodes and include the view id in the
         // exception message to make sure we got the right rehash
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPrepareCommand(ctx, cmd);
      } finally {
         stateTransferLock.releaseForCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand cmd) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitRollbackCommand(ctx, cmd);
      } finally {
         stateTransferLock.releaseForCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand cmd) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitCommitCommand(ctx, cmd);
      } finally {
         stateTransferLock.releaseForCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand cmd) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, cmd)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitLockControlCommand(ctx, cmd);
      } finally {
         stateTransferLock.releaseForCommand(ctx, cmd);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPutKeyValueCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitRemoveCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitReplaceCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitClearCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         throw new RehashInProgressException("Timed out waiting for the transaction lock");
      }
      try {
         return super.visitPutMapCommand(ctx, command);
      } finally {
         stateTransferLock.releaseForCommand(ctx, command);
      }
   }


}
