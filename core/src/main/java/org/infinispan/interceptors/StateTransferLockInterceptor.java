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

import org.infinispan.CacheException;
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
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.StateTransferInProgressException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferLockReacquisitionException;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * An interceptor that blocks any commands when the {@link StateTransferLock} is locked.
 * <br/>
 * To make the state transfer as short as possible, synchronous remote commands don't wait
 * at all for the state transfer lock. So it's the originator's job to retry the command
 * after the state transfer has ended. This interceptor handles the retries in {@code handleWithRetries}.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public class StateTransferLockInterceptor extends CommandInterceptor {

   StateTransferLock stateTransferLock;
   private long rpcTimeout;

   private static final Log log = LogFactory.getLog(StateTransferLockInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration) {
      this.stateTransferLock = stateTransferLock;
      // no need to retry for asynchronous caches
      this.rpcTimeout = configuration.getCacheMode().isSynchronous()
            ? configuration.getSyncReplTimeout() : 0;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         return signalStateTransferInProgress(ctx);
      }
      boolean release = true;
      try {
         return handleWithRetries(ctx, command, rpcTimeout);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         return signalStateTransferInProgress(ctx);
      }
      boolean release = true;
      try {
         return handleWithRetries(ctx, command, rpcTimeout);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         return signalStateTransferInProgress(ctx);
      }
      boolean release = true;
      try {
         return handleWithRetries(ctx, command, -1);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         return signalStateTransferInProgress(ctx);
      }
      boolean release = true;
      try {
         return handleWithRetries(ctx, command, rpcTimeout);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (!stateTransferLock.acquireForCommand(ctx, command)) {
         return signalStateTransferInProgress(ctx);
      }
      boolean release = true;
      try {
         return handleWithRetries(ctx, command, rpcTimeout);
      } catch (StateTransferLockReacquisitionException e) {
         release = false;
         return signalStateTransferInProgress(ctx);
      } finally {
         if (release) {
            stateTransferLock.releaseForCommand(ctx, command);
         }
      }
   }

   /**
    * On the originator if we time out acquiring the state transfer lock the caller will see a {@code StateTransferInProgressException},
    * which extends {@code TimeoutException}.
    * If this happens on a remote node however the originator will catch the exception and retry the command.
    * @return
    */
   private Object signalStateTransferInProgress(InvocationContext ctx) {
         int viewId = stateTransferLock.getBlockingCacheViewId();
      if (ctx.isOriginLocal()) {
         throw new StateTransferInProgressException(viewId, "Timed out waiting for the state transfer lock, state transfer in progress for view " + viewId);
      } else {
         throw new StateTransferInProgressException(viewId, "State transfer in progress on target node for view " + viewId
               + ", returning to the originator to allow state transfer to proceed.");
      }
   }

   private Object handleWithRetries(InvocationContext ctx, VisitableCommand command, long timeoutMillis) throws Throwable {
      long endNanos = timeoutMillis > 0 ? (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)) : Long.MAX_VALUE;
      while (true) {
         int newCacheViewId = -1;
         try {
            return invokeNextInterceptor(ctx, command);
         } catch (StateTransferInProgressException e) {
            newCacheViewId = e.getNewCacheViewId();
            log.debugf("Caught StateTransferInProgressException, waiting for the state transfer %d to start", newCacheViewId);
         } catch (SuspectException e) {
            // a node has left, that means the coordinator will soon install a new cache view
            newCacheViewId = newCacheViewId + 1;
            log.debugf("Caught SuspectException, waiting for the state transfer %d to start", newCacheViewId);
         }
         if (endNanos < System.nanoTime()) {
            throw new TimeoutException("Timed out waiting for the state transfer to end");
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
