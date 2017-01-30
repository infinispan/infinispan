package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.BackupPutMapRcpCommand;
import org.infinispan.commands.write.BackupWriteRcpCommand;
import org.infinispan.commands.write.PrimaryWriteRpcCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.CheckTopologyAction;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.LockAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.inboundhandler.action.TriangleOrderAction;
import org.infinispan.statetransfer.StateRequestCommand;
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
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements
      LockListener {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CheckTopologyAction checkTopologyAction;

   private LockManager lockManager;
   @SuppressWarnings("deprecation")
   private ClusteringDependentLogic clusteringDependentLogic;
   private TriangleOrderManager triangleOrderManager;
   private long lockTimeout;

   public NonTotalOrderPerCacheInboundInvocationHandler() {
      checkTopologyAction = new CheckTopologyAction(this);
   }

   @Inject
   public void inject(LockManager lockManager,
         @SuppressWarnings("deprecation") ClusteringDependentLogic clusteringDependentLogic,
         Configuration configuration, TriangleOrderManager triangleOrderManager) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.triangleOrderManager = triangleOrderManager;
      lockTimeout = configuration.locking().lockAcquisitionTimeout();
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean onExecutorService = executeOnExecutorService(order, command);
         final boolean sync = order.preserveOrder();
         final BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               runnable = onExecutorService ?
                     createReadyActionRunnable(command, reply, commandTopologyId, sync,
                           createReadyAction(commandTopologyId, (SingleRpcCommand) command)) :
                     createDefaultRunnable(command, reply, commandTopologyId, TopologyMode.WAIT_TX_DATA, sync);
               break;
            case BackupWriteRcpCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, sync, createReadyAction(
                     (BackupWriteRcpCommand) command, commandTopologyId));
               break;
            case BackupPutMapRcpCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, sync,
                     createReadyAction((BackupPutMapRcpCommand) command, commandTopologyId));
               break;
            case PrimaryWriteRpcCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, sync,
                     createReadyAction(commandTopologyId, (PrimaryWriteRpcCommand) command));
               break;
            default:
               runnable = createDefaultRunnable(command, reply, commandTopologyId,
                     command.getCommandId() != StateRequestCommand.COMMAND_ID, onExecutorService, sync);
               break;
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   @Override
   public void onEvent(LockState state) {
      remoteCommandsExecutor.checkForReadyTasks();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private BlockingRunnable createReadyActionRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
         boolean sync, ReadyAction readyAction) {
      if (readyAction != null) {
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
      } else {
         return new DefaultTopologyRunnable(this, command, reply, TopologyMode.READY_TX_DATA, commandTopologyId, sync);
      }
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
            checkTopologyAction,
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

   private ReadyAction createReadyAction(BackupWriteRcpCommand command, int topologyId) {
      return createTriangleOrderAction(command, topologyId, command.getSequence(), command.getKey());
   }

   private ReadyAction createReadyAction(BackupPutMapRcpCommand command, int topologyId) {
      return createTriangleOrderAction(command, topologyId, command.getSequence(),
            command.getMap().keySet().iterator().next());
   }

   private ReadyAction createTriangleOrderAction(ReplicableCommand command, int topologyId, long sequence, Object key) {
      return new DefaultReadyAction(new ActionState(command, topologyId, 0), checkTopologyAction,
            new TriangleOrderAction(triangleOrderManager, remoteCommandsExecutor, clusteringDependentLogic, sequence,
                  key));
   }
}
