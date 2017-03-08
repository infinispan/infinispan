package org.infinispan.remoting.inboundhandler;

import static org.infinispan.remoting.inboundhandler.DeliverOrder.NONE;

import java.util.Collection;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.BackupPutMapRcpCommand;
import org.infinispan.commands.write.BackupWriteRcpCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.Action;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.ActionStatus;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.LockAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.inboundhandler.action.TriangleOrderAction;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CommandAckCollector;
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
   private ClusteringDependentLogic clusteringDependentLogic;
   private long lockTimeout;
   private TriangleOrderManager triangleOrderManager;
   private RpcManager rpcManager;
   private CommandAckCollector commandAckCollector;
   private CommandsFactory commandsFactory;
   private Address localAddress;

   @Inject
   public void inject(LockManager lockManager,
         ClusteringDependentLogic clusteringDependentLogic,
         Configuration configuration, TriangleOrderManager anotherTriangleOrderManager, RpcManager rpcManager,
         CommandAckCollector commandAckCollector, CommandsFactory commandsFactory) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      lockTimeout = configuration.locking().lockAcquisitionTimeout();
      this.triangleOrderManager = anotherTriangleOrderManager;
      this.rpcManager = rpcManager;
      this.commandAckCollector = commandAckCollector;
      this.commandsFactory = commandsFactory;
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
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
               handleBackupWriteRpcCommand((BackupWriteRcpCommand) command);
               return;
            case BackupPutMapRcpCommand.COMMAND_ID:
               handleBackupPutMapRpcCommand((BackupPutMapRcpCommand) command);
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

   public TriangleOrderManager getTriangleOrderManager() {
      return triangleOrderManager;
   }

   public BlockingTaskAwareExecutorService getRemoteExecutor() {
      return remoteCommandsExecutor;
   }

   public ClusteringDependentLogic getClusteringDependentLogic() {
      return clusteringDependentLogic;
   }

   @Override
   public void onFinally(ActionState state) {
      //no-op
      //needed for ConditionalOperationPrimaryOwnerFailTest
      //it mocks this class and when Action.onFinally is invoked, it doesn't behave well with the default implementation
      //in the interface.
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

   private void handleBackupPutMapRpcCommand(BackupPutMapRcpCommand command) {
      final int topologyId = command.getTopologyId();
      ReadyAction readyAction = createTriangleOrderAction(command, topologyId, command.getSequence(),
            command.getMap().keySet().iterator().next());
      BlockingRunnable runnable = createBackupPutMapRunnable(command, topologyId, readyAction);
      remoteCommandsExecutor.execute(runnable);
   }

   private void handleBackupWriteRpcCommand(BackupWriteRcpCommand command) {
      final int topologyId = command.getTopologyId();
      ReadyAction readyAction = createTriangleOrderAction(command, topologyId, command.getSequence(), command.getKey());
      BlockingRunnable runnable = createBackupWriteRpcRunnable(command, topologyId, readyAction);
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

   private void sendExceptionAck(CommandInvocationId id, Throwable throwable, int topologyId) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id.getId(), throwable, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildExceptionAckCommand(id.getId(), throwable, topologyId), NONE);
      }
   }

   private void sendBackupAck(CommandInvocationId id, int topologyId) {
      final Address origin = id.getAddress();
      boolean isLocal = localAddress.equals(origin);
      if (trace) {
         log.tracef("Sending ack for command %s. isLocal? %s.", id, isLocal);
      }
      if (isLocal) {
         commandAckCollector.backupAck(id.getId(), origin, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildBackupAckCommand(id.getId(), topologyId), NONE);
      }
   }

   private BlockingRunnable createBackupWriteRpcRunnable(BackupWriteRcpCommand command, int commandTopologyId,
         ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, Reply.NO_OP, TopologyMode.READY_TX_DATA, commandTopologyId,
            false) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
            readyAction.onFinally(); //notified TriangleOrderManager before sending the ack.
            sendExceptionAck(((BackupWriteRcpCommand) command).getCommandInvocationId(), throwable, commandTopologyId);
         }

         @Override
         protected void afterInvoke() {
            super.afterInvoke();
            readyAction.onFinally();
            sendBackupAck(((BackupWriteRcpCommand) command).getCommandInvocationId(), commandTopologyId);
         }
      };
   }

   private void sendPutMapBackupAck(CommandInvocationId id, int topologyId, int segment) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.multiKeyBackupAck(id.getId(), localAddress, segment, topologyId);
      } else {
         rpcManager
               .sendTo(origin, commandsFactory.buildBackupMultiKeyAckCommand(id.getId(), segment, topologyId), NONE);
      }
   }

   private BlockingRunnable createBackupPutMapRunnable(BackupPutMapRcpCommand command, int commandTopologyId,
         ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, Reply.NO_OP, TopologyMode.READY_TX_DATA, commandTopologyId,
            false) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
            readyAction.onFinally();
            sendExceptionAck(((BackupPutMapRcpCommand) command).getCommandInvocationId(), throwable, commandTopologyId);
         }

         @Override
         protected void afterInvoke() {
            super.afterInvoke();
            readyAction.onFinally();
            Object key = ((BackupPutMapRcpCommand) command).getMap().keySet().iterator().next();
            int segment = clusteringDependentLogic.getCacheTopology().getDistribution(key).segmentId();
            sendPutMapBackupAck(((BackupPutMapRcpCommand) command).getCommandInvocationId(), commandTopologyId,
                  segment);
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

   private ReadyAction createTriangleOrderAction(ReplicableCommand command, int topologyId, long sequence, Object key) {
      return new DefaultReadyAction(new ActionState(command, topologyId, 0), this,
            new TriangleOrderAction(this, sequence, key));
   }
}
