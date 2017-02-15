package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.BackupPutMapRcpCommand;
import org.infinispan.commands.write.BackupWriteRcpCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.AnotherTriangleOrderManager;
import org.infinispan.distribution.CommandPosition;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.Action;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.ActionStatus;
import org.infinispan.remoting.inboundhandler.action.AnotherTriangleOrderAction;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.LockAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-transactional and distributed caches that uses the
 * triangle algorithm.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TrianglePerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements
      LockListener, Action {

   private static final Log log = LogFactory.getLog(TrianglePerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private LockManager lockManager;
   @SuppressWarnings("deprecation")
   private ClusteringDependentLogic clusteringDependentLogic;
   private long lockTimeout;
   private AnotherTriangleOrderManager triangleOrderManager;

   @Inject
   public void inject(LockManager lockManager,
         @SuppressWarnings("deprecation") ClusteringDependentLogic clusteringDependentLogic,
         Configuration configuration, AnotherTriangleOrderManager anotherTriangleOrderManager) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      lockTimeout = configuration.locking().lockAcquisitionTimeout();
      this.triangleOrderManager = anotherTriangleOrderManager;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               handleSingleRpcCommand((SingleRpcCommand) command, reply, order);
               return;
            case BackupWriteRcpCommand.COMMAND_ID:
               handleBackupWriteRpcCommand((BackupWriteRcpCommand) command, reply);
               return;
            case BackupPutMapRcpCommand.COMMAND_ID:
               handleBackupPutMapRpcCommand((BackupPutMapRcpCommand) command, reply);
               return;
            case PrimaryAckCommand.COMMAND_ID:
               handlePrimaryAckCommand((PrimaryAckCommand) command);
               return;
            case PrimaryMultiKeyAckCommand.COMMAND_ID:
               handlePrimaryMultiKeyAckCommand((PrimaryMultiKeyAckCommand) command);
               return;
            case BackupAckCommand.COMMAND_ID:
               handleBackupAckCommand((BackupAckCommand) command);
               return;
            case BackupMultiKeyAckCommand.COMMAND_ID:
               handleBackupMultiKeyAckCommand((BackupMultiKeyAckCommand) command);
               return;
            case ExceptionAckCommand.COMMAND_ID:
               handleExceptionAck((ExceptionAckCommand) command);
               return;
            case StateRequestCommand.COMMAND_ID:
               handleStateRequestCommand((StateRequestCommand) command, reply, order);
               return;
            default:
               handleDefaultCommand(command, reply, order);
         }
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   //lock listener interface
   @Override
   public void onEvent(LockState state) {
      remoteCommandsExecutor.checkForReadyTasks();
   }

   //action interface
   @Override
   public ActionStatus check(ActionState state) {
      return isCommandSentBeforeFirstTopology(state.getCommandTopologyId()) ?
            ActionStatus.CANCELED :
            ActionStatus.READY;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private void handleStateRequestCommand(StateRequestCommand command, Reply reply, DeliverOrder order) {
      if (executeOnExecutorService(order, command)) {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.READY_TOPOLOGY, order.preserveOrder());
         remoteCommandsExecutor.execute(runnable);
      } else {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.WAIT_TOPOLOGY, order.preserveOrder());
         runnable.run();
      }
   }

   private void handleDefaultCommand(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (executeOnExecutorService(order, command)) {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.READY_TX_DATA, order.preserveOrder());
         remoteCommandsExecutor.execute(runnable);
      } else {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.WAIT_TX_DATA, order.preserveOrder());
         runnable.run();
      }
   }

   private void handleBackupPutMapRpcCommand(BackupPutMapRcpCommand command, Reply reply) {
      handleBackup(command, command.getTopologyId(), triangleOrderManager.orderMultipleKeys(command.getMap().keySet()), reply);
   }

   private void handleBackupWriteRpcCommand(BackupWriteRcpCommand command, Reply reply) {
      handleBackup(command, command.getTopologyId(), triangleOrderManager.orderKey(command.getKey()), reply);
   }

   private void handleBackup(CacheRpcCommand command, int topologyId, CommandPosition position, Reply reply) {
      ReadyAction readyAction = createTriangleOrderAction(command, topologyId, position);
      BlockingRunnable runnable = createNonNullReadyActionRunnable(command, reply, topologyId, false, readyAction);
      remoteCommandsExecutor.execute(runnable);
   }

   private void handleExceptionAck(ExceptionAckCommand command) {
      command.ack();
   }

   private void handleBackupMultiKeyAckCommand(BackupMultiKeyAckCommand command) {
      command.ack();
   }

   private void handleBackupAckCommand(BackupAckCommand command) {
      command.ack();
   }

   private void handlePrimaryMultiKeyAckCommand(PrimaryMultiKeyAckCommand command) {
      command.ack();
   }

   private void handlePrimaryAckCommand(PrimaryAckCommand command) {
      command.ack();
   }

   private void handleSingleRpcCommand(SingleRpcCommand command, Reply reply, DeliverOrder order) {
      if (executeOnExecutorService(order, command)) {
         int commandTopologyId = extractCommandTopologyId(command);
         BlockingRunnable runnable = createReadyActionRunnable(command, reply, commandTopologyId, order.preserveOrder(),
               createReadyAction(commandTopologyId, command));
         remoteCommandsExecutor.execute(runnable);
      } else {
         createDefaultRunnable(command, reply, extractCommandTopologyId(command), TopologyMode.WAIT_TX_DATA,
               order.preserveOrder()).run();
      }
   }

   private BlockingRunnable createReadyActionRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
         boolean sync, ReadyAction readyAction) {
      if (readyAction != null) {
         return createNonNullReadyActionRunnable(command, reply, commandTopologyId, sync, readyAction);
      } else {
         return new DefaultTopologyRunnable(this, command, reply, TopologyMode.READY_TX_DATA, commandTopologyId, sync);
      }
   }

   private BlockingRunnable createNonNullReadyActionRunnable(CacheRpcCommand command
         , Reply reply, int commandTopologyId, boolean sync, ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, reply, TopologyMode.READY_TX_DATA, commandTopologyId, sync) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
         }

         @Override
         protected void onFinally() {
            super.onFinally();
            readyAction.onFinally();
         }
      };
   }

   private ReadyAction createReadyAction(int topologyId, RemoteLockCommand command) {
      if (command.hasSkipLocking()) {
         return null;
      }
      Collection<?> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockTimeout;

      DefaultReadyAction action = new DefaultReadyAction(new ActionState(command, topologyId, timeoutMillis),
            this,
            new LockAction(lockManager, clusteringDependentLogic));
      action.registerListener();
      return action;
   }

   private ReadyAction createReadyAction(int topologyId, SingleRpcCommand singleRpcCommand) {
      ReplicableCommand command = singleRpcCommand.getCommand();
      return command instanceof RemoteLockCommand ?
            createReadyAction(topologyId, (RemoteLockCommand & ReplicableCommand) command) :
            null;
   }

   private ReadyAction createTriangleOrderAction(ReplicableCommand command, int topologyId, CommandPosition position) {
      return new DefaultReadyAction(new ActionState(command, topologyId, 0), this,
            new AnotherTriangleOrderAction(remoteCommandsExecutor, position));
   }
}
