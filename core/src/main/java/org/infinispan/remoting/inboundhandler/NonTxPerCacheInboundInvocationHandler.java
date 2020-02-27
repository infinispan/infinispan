package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.ScatteredStateConfirmRevokedCommand;
import org.infinispan.commands.statetransfer.ScatteredStateGetKeysCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.CheckTopologyAction;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements
      LockListener {

   private static final Log log = LogFactory.getLog(NonTxPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CheckTopologyAction checkTopologyAction;

   @Inject LockManager lockManager;
   @Inject DistributionManager distributionManager;

   private long lockTimeout;
   private boolean isLocking;

   public NonTxPerCacheInboundInvocationHandler() {
      checkTopologyAction = new CheckTopologyAction(this);
   }

   @Override
   public void start() {
      super.start();
      lockTimeout = configuration.locking().lockAcquisitionTimeout();
      isLocking = !configuration.clustering().cacheMode().isScattered();
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
            case SingleRpcCommand.COMMAND_ID:
               runnable = onExecutorService ?
                     createReadyActionRunnable(command, reply, commandTopologyId, sync,
                           createReadyAction(commandTopologyId, (SingleRpcCommand) command)) :
                     createDefaultRunnable(command, reply, commandTopologyId, TopologyMode.WAIT_TX_DATA, sync);
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
   public void onEvent(LockState state) {
      checkForReadyTasks();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private ReadyAction createReadyAction(int topologyId, RemoteLockCommand command) {
      if (command.hasSkipLocking() || !isLocking) {
         return null;
      }
      Collection<?> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockTimeout;

      DefaultReadyAction action = new DefaultReadyAction(new ActionState(command, topologyId, timeoutMillis),
            checkTopologyAction);
      action.registerListener();
      return action;
   }

   private ReadyAction createReadyAction(int topologyId, SingleRpcCommand singleRpcCommand) {
      ReplicableCommand command = singleRpcCommand.getCommand();
      return command instanceof RemoteLockCommand ?
            createReadyAction(topologyId, (RemoteLockCommand & ReplicableCommand) command) :
            null;
   }
}
