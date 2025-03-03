package org.infinispan.remoting.inboundhandler;

import static org.infinispan.commons.util.EnumUtil.containsAll;
import static org.infinispan.context.impl.FlagBitSets.FORCE_ASYNCHRONOUS;
import static org.infinispan.context.impl.FlagBitSets.FORCE_SYNCHRONOUS;
import static org.infinispan.remoting.inboundhandler.DeliverOrder.NONE_NO_FC;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.statetransfer.StateTransferCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.inboundhandler.action.TriangleOrderAction;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-transactional and distributed caches that uses the
 * triangle algorithm.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TrianglePerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(TrianglePerCacheInboundInvocationHandler.class);

   @Inject TriangleOrderManager triangleOrderManager;
   @Inject CommandAckCollector commandAckCollector;
   @Inject CommandsFactory commandsFactory;

   private Address localAddress;
   private boolean asyncCache;

   @Override
   public void start() {
      super.start();
      localAddress = rpcManager.getAddress();
      asyncCache = !configuration.clustering().cacheMode().isSynchronous();
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         if (command instanceof SingleRpcCommand) {
            handleSingleRpcCommand((SingleRpcCommand) command, reply, order);
         } else if (command instanceof BackupWriteCommand bwc) {
            handleBackupWriteCommand(bwc);
         } else if (command instanceof BackupMultiKeyAckCommand bmkac) {
            bmkac.ack(commandAckCollector);
         } else if (command instanceof ExceptionAckCommand eac) {
            eac.ack(commandAckCollector);
         } else if (command instanceof StateTransferCommand) {
            handleStateRequestCommand(command, reply, order);
         } else {
         handleDefaultCommand(command, reply, order);
         }
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   public TriangleOrderManager getTriangleOrderManager() {
      return triangleOrderManager;
   }

   private void handleStateRequestCommand(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (executeOnExecutorService(order, command)) {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.READY_TOPOLOGY, order.preserveOrder());
         blockingExecutor.execute(runnable);
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
         blockingExecutor.execute(runnable);
      } else {
         BlockingRunnable runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command),
               TopologyMode.WAIT_TX_DATA, order.preserveOrder());
         runnable.run();
      }
   }

   private void handleBackupWriteCommand(BackupWriteCommand command) {
      final int topologyId = command.getTopologyId();
      ReadyAction readyAction = createTriangleOrderAction(command, topologyId, command.getSequence(),
            command.getSegmentId());
      BlockingRunnable runnable = createBackupWriteRunnable(command, topologyId, readyAction);
      nonBlockingExecutor.execute(runnable);
   }

   private void handleSingleRpcCommand(SingleRpcCommand command, Reply reply, DeliverOrder order) {
      if (executeOnExecutorService(order, command)) {
         int commandTopologyId = extractCommandTopologyId(command);
         BlockingRunnable runnable = createDefaultRunnable(command, reply, commandTopologyId,
                                                           TopologyMode.READY_TX_DATA,
                                                           order.preserveOrder());
         blockingExecutor.execute(runnable);
      } else {
         createDefaultRunnable(command, reply, extractCommandTopologyId(command), TopologyMode.WAIT_TX_DATA,
               order.preserveOrder()).run();
      }
   }

   private void sendExceptionAck(CommandInvocationId id, Throwable throwable, int topologyId, long flagBitSet) {
      final Address origin = id.getAddress();
      if (skipBackupAck(flagBitSet)) {
         if (log.isTraceEnabled()) {
            log.tracef("Skipping ack for command %s.", id);
         }
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id.getId(), throwable, topologyId);
      } else {
         rpcManager.sendTo(origin, commandsFactory.buildExceptionAckCommand(id.getId(), throwable, topologyId), NONE_NO_FC);
      }
   }

   private void onBackupException(BackupWriteCommand command, Throwable throwable, ReadyAction readyAction) {
      readyAction.onException();
      readyAction.onFinally(); //notified TriangleOrderManager before sending the ack.
      sendExceptionAck(command.getCommandInvocationId(), throwable, command.getTopologyId(), command.getFlags());
   }

   private void sendBackupAck(CommandInvocationId id, int topologyId, int segment, long flagBitSet) {
      final Address origin = id.getAddress();
      if (skipBackupAck(flagBitSet)) {
         if (log.isTraceEnabled()) {
            log.tracef("Skipping ack for command %s.", id);
         }
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.backupAck(id.getId(), localAddress, segment, topologyId);
      } else {
         BackupMultiKeyAckCommand command =
               commandsFactory.buildBackupMultiKeyAckCommand(id.getId(), segment, topologyId);
         rpcManager.sendTo(origin, command, NONE_NO_FC);
      }
   }

   private BlockingRunnable createBackupWriteRunnable(BackupWriteCommand command, int commandTopologyId,
                                                      ReadyAction readyAction) {
      readyAction.addListener(this::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, Reply.NO_OP, TopologyMode.READY_TX_DATA, commandTopologyId,
            false) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            onBackupException((BackupWriteCommand) command, throwable, readyAction);
         }

         @Override
         protected void afterInvoke() {
            super.afterInvoke();
            readyAction.onFinally();
            BackupWriteCommand backupCommand = (BackupWriteCommand) command;
            sendBackupAck(backupCommand.getCommandInvocationId(), commandTopologyId, backupCommand.getSegmentId(),
                  backupCommand.getFlags());
         }
      };
   }

   private ReadyAction createTriangleOrderAction(ReplicableCommand command, int topologyId, long sequence, int segmentId) {
      return new DefaultReadyAction(new ActionState(command, topologyId),
            new TriangleOrderAction(this, sequence, segmentId));
   }

   private boolean skipBackupAck(long flagBitSet) {
      return containsAll(flagBitSet, FORCE_ASYNCHRONOUS) ||
            (asyncCache && !containsAll(flagBitSet, FORCE_SYNCHRONOUS));
   }
}
