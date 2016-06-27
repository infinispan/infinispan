package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.CheckTopologyAction;
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

import java.util.Collection;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements LockListener {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CheckTopologyAction checkTopologyAction;

   private LockManager lockManager;
   private ClusteringDependentLogic clusteringDependentLogic;
   private Configuration configuration;

   public NonTotalOrderPerCacheInboundInvocationHandler() {
      checkTopologyAction = new CheckTopologyAction(this);
   }

   @Inject
   public void inject(LockManager lockManager, ClusteringDependentLogic clusteringDependentLogic, Configuration configuration) {
      this.lockManager = lockManager;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.configuration = configuration;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean onExecutorService = executeOnExecutorService(order, command);
         final BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, true, onExecutorService,
                                                    createReadyActionForSingleRpcCommand(commandTopologyId, (SingleRpcCommand) command));
               break;
            default:
               runnable = createDefaultRunnable(command, reply, commandTopologyId, command.getCommandId() != StateRequestCommand.COMMAND_ID, onExecutorService);
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
                                                              boolean waitTransactionalData, boolean onExecutorService,
                                                              ReadyAction readyAction) {
      final TopologyMode topologyMode = TopologyMode.create(onExecutorService, waitTransactionalData);
      if (onExecutorService && readyAction != null) {
         readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId) {
            @Override
            public boolean isReady() {
               return super.isReady() && readyAction.isReady();
            }

            @Override
            protected void onFinally() {
               super.onFinally();
               readyAction.cleanup();
            }
         };
      } else {
         return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId);
      }
   }

   private ReadyAction createReadyAction(int topologyId, RemoteLockCommand command) {
      if (command.hasSkipLocking()) {
         return null;
      }
      Collection<Object> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : configuration.locking().lockAcquisitionTimeout();

      DefaultReadyAction action = new DefaultReadyAction(new ActionState(command, topologyId, timeoutMillis),
                                                         checkTopologyAction,
                                                         new LockAction(lockManager, clusteringDependentLogic));
      action.registerListener();
      return action;
   }

   private ReadyAction createReadyActionForSingleRpcCommand(int topologyId, SingleRpcCommand singleRpcCommand) {
      ReplicableCommand command = singleRpcCommand.getCommand();
      return command instanceof RemoteLockCommand ? createReadyAction(topologyId, (RemoteLockCommand) command) : null;
   }
}
