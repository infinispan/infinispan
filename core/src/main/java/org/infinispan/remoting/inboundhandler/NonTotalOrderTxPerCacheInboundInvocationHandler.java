package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.CheckTopologyAction;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.LockAction;
import org.infinispan.remoting.inboundhandler.action.PendingTxAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-total order caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(NonTotalOrderTxPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   private final CheckTopologyAction checkTopologyAction;

   @Inject private LockManager lockManager;
   @Inject private ClusteringDependentLogic clusteringDependentLogic;
   @Inject private PendingLockManager pendingLockManager;

   private boolean pessimisticLocking;
   private long lockAcquisitionTimeout;

   public NonTotalOrderTxPerCacheInboundInvocationHandler() {
      checkTopologyAction = new CheckTopologyAction(this);
   }

   @Start
   public void start() {
      this.pessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      this.lockAcquisitionTimeout = configuration.locking().lockAcquisitionTimeout();
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
            case PrepareCommand.COMMAND_ID:
            case VersionedPrepareCommand.COMMAND_ID:
               if (pessimisticLocking) {
                  runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService, sync);
               } else {
                  runnable = createReadyActionRunnable(command, reply, commandTopologyId, onExecutorService,
                        sync, createReadyAction(commandTopologyId, (PrepareCommand) command)
                  );
               }
               break;
            case LockControlCommand.COMMAND_ID:
               runnable = createReadyActionRunnable(command, reply, commandTopologyId, onExecutorService,
                     sync, createReadyAction(commandTopologyId, (LockControlCommand) command)
               );
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
         readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
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
                                                         checkTopologyAction,
                                                         new PendingTxAction(pendingLockManager, clusteringDependentLogic),
                                                         new LockAction(lockManager, clusteringDependentLogic));
      action.registerListener();
      return action;
   }
}
