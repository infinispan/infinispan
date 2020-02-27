package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.ScatteredStateConfirmRevokedCommand;
import org.infinispan.commands.statetransfer.ScatteredStateGetKeysCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.CheckTopologyAction;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-total order caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class TxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(TxPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CheckTopologyAction checkTopologyAction;

   private boolean pessimisticLocking;
   private long lockAcquisitionTimeout;

   public TxPerCacheInboundInvocationHandler() {
      checkTopologyAction = new CheckTopologyAction(this);
   }

   @Override
   public void start() {
      super.start();
      this.pessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      this.lockAcquisitionTimeout = configuration.locking().lockAcquisitionTimeout();
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean onExecutorService = executeOnExecutorService(order, command);
         final boolean sync = order.preserveOrder();
         final BlockingRunnable runnable;

         boolean waitForTransactionalData = true;
         switch (command.getCommandId()) {
            case PrepareCommand.COMMAND_ID:
            case VersionedPrepareCommand.COMMAND_ID:
               if (pessimisticLocking) {
                  runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService, sync);
               } else {
                  runnable = createReadyActionRunnable(command, reply, commandTopologyId, onExecutorService,
                        sync, createReadyAction(commandTopologyId, (PrepareCommand) command));
               }
               break;
            case LockControlCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, onExecutorService,
                     sync, createReadyAction(commandTopologyId, (LockControlCommand) command)
               );
               break;
            case ConflictResolutionStartCommand.COMMAND_ID:
            case ScatteredStateConfirmRevokedCommand.COMMAND_ID:
            case ScatteredStateGetKeysCommand.COMMAND_ID:
            case StateTransferCancelCommand.COMMAND_ID:
            case StateTransferGetListenersCommand.COMMAND_ID:
            case StateTransferGetTransactionsCommand.COMMAND_ID:
            case StateTransferStartCommand.COMMAND_ID:
               waitForTransactionalData = false;
            default:
               runnable = createDefaultRunnable(command, reply, commandTopologyId, waitForTransactionalData, onExecutorService, sync);
               break;
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private BlockingRunnable createReadyActionRunnable(CacheRpcCommand command, Reply reply,
         int commandTopologyId, boolean onExecutorService, boolean sync, ReadyAction readyAction) {
      final TopologyMode topologyMode = TopologyMode.create(onExecutorService, true);
      if (onExecutorService && readyAction != null) {
         readyAction.addListener(this::checkForReadyTasks);
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId, sync) {
            @Override
            public boolean isReady() {
               return super.isReady() && readyAction.isReady();
            }
         };
      } else {
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId, sync);
      }
   }

   private ReadyAction createReadyAction(int topologyId, TransactionalRemoteLockCommand replicableCommand) {
      if (replicableCommand.hasSkipLocking()) {
         return null;
      }
      Collection<?> keys = replicableCommand.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      final long timeoutMillis = replicableCommand.hasZeroLockAcquisition() ? 0 : lockAcquisitionTimeout;

      DefaultReadyAction action = new DefaultReadyAction(new ActionState(replicableCommand, topologyId, timeoutMillis),
                                                         checkTopologyAction);
      action.registerListener();
      return action;
   }
}
