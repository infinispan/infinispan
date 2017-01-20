package org.infinispan.interceptors;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * It handles the acknowledges for the triangle algorithm (distributed mode only!)
 * <p>
 * It is placed between the {@link org.infinispan.statetransfer.StateTransferInterceptor} and the {@link
 * org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor}.
 * <p>
 * The acknowledges are sent after the lock is released and it interacts with the {@link
 * org.infinispan.statetransfer.StateTransferInterceptor} to trigger the retries when topology changes while the
 * commands are processing.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleAckInterceptor extends DDAsyncInterceptor {

   private static final Log log = LogFactory.getLog(TriangleAckInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport transport;
   private CommandsFactory commandsFactory;
   private CommandAckCollector commandAckCollector;
   private final InvocationComposeHandler onLocalWriteCommand = this::onLocalWriteCommand;
   private DistributionManager distributionManager;
   private StateTransferManager stateTransferManager;
   private Address localAddress;
   private final InvocationComposeHandler onRemotePrimaryOwner = this::onRemotePrimaryOwner;
   private final InvocationComposeHandler onRemoteBackupOwner = this::onRemoteBackupOwner;

   @Inject
   public void inject(Transport transport, CommandsFactory commandsFactory, CommandAckCollector commandAckCollector,
         DistributionManager distributionManager, StateTransferManager stateTransferManager) {
      this.transport = transport;
      this.commandsFactory = commandsFactory;
      this.commandAckCollector = commandAckCollector;
      this.distributionManager = distributionManager;
      this.stateTransferManager = stateTransferManager;
   }

   @Start
   public void start() {
      localAddress = transport.getAddress();
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command);
      } else {
         return handleRemotePutMapCommand(ctx, command);
      }
   }

   private BasicInvocationStage handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (t != null) {
            sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), t);
            return stage;
         }
         int segment = distributionManager.getConsistentHash().getSegment(command.getMap().keySet().iterator().next());
         if (cmd.isForwarded()) {
            sendPutMapBackupAck(cmd, segment);
         } else {
            //noinspection unchecked
            sendPrimaryPutMapAck(cmd, (Map<Object, Object>) rv);
         }
         return stage;
      });
   }

   private BasicInvocationStage handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, throwable) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (throwable != null) {
            disposeCollectorOnException(cmd.getCommandInvocationId());
            return stage;
         }
         return waitCollectorAsync(stage, cmd.getCommandInvocationId());
      });
   }


   private BasicInvocationStage handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Exception {
      if (ctx.isOriginLocal()) {
         return invokeNext(ctx, command).compose(onLocalWriteCommand);
      } else {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         if (command.getTopologyId() != cacheTopology.getTopologyId()) {
            sendExceptionAck(command.getCommandInvocationId(), command.getTopologyId(),
                  OutdatedTopologyException.getCachedInstance());
            throw OutdatedTopologyException.getCachedInstance();
         }
         DistributionInfo distributionInfo = new DistributionInfo(command.getKey(),
               cacheTopology.getWriteConsistentHash(), localAddress);
         switch (distributionInfo.ownership()) {
            case BACKUP:
               return invokeNext(ctx, command).compose(onRemoteBackupOwner);
            case PRIMARY:
               return invokeNext(ctx, command).compose(onRemotePrimaryOwner);
            default:
               throw new IllegalStateException();
         }
      }
   }

   @SuppressWarnings("unused")
   private BasicInvocationStage onRemotePrimaryOwner(BasicInvocationStage stage, InvocationContext rCtx,
         VisitableCommand rCommand, Object rv, Throwable throwable) throws Exception {
      DataWriteCommand command = (DataWriteCommand) rCommand;
      if (throwable != null) {
         sendExceptionAck(command.getCommandInvocationId(), command.getTopologyId(), throwable);
      } else {
         sendPrimaryAck(command, rv);
      }
      return stage;
   }

   @SuppressWarnings("unused")
   private BasicInvocationStage onRemoteBackupOwner(BasicInvocationStage stage, InvocationContext rCtx,
         VisitableCommand rCommand, Object rv, Throwable throwable) throws Exception {
      DataWriteCommand cmd = (DataWriteCommand) rCommand;
      if (throwable != null) {
         sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), throwable);
      } else {
         sendBackupAck(cmd);
      }
      return stage;
   }

   @SuppressWarnings("unused")
   private BasicInvocationStage onLocalWriteCommand(BasicInvocationStage stage, InvocationContext rCtx,
         VisitableCommand rCommand, Object rv, Throwable throwable) {
      final DataWriteCommand cmd = (DataWriteCommand) rCommand;
      if (throwable != null) {
         disposeCollectorOnException(cmd.getCommandInvocationId());
         return stage;
      }
      return waitCollectorAsync(stage, cmd.getCommandInvocationId());
   }

   private void disposeCollectorOnException(CommandInvocationId id) {
      //a local exception occur. No need to wait for acknowledges.
      commandAckCollector.dispose(id.getId());
   }

   private BasicInvocationStage waitCollectorAsync(BasicInvocationStage stage, CommandInvocationId id) {
      //waiting for acknowledges based on default rpc timeout.
      CompletableFuture<Object> collectorFuture = commandAckCollector.getCollectorCompletableFutureToWait(id.getId());
      if (collectorFuture == null) {
         //no collector, return immediately.
         return stage;
      }
      return returnWithAsync(collectorFuture);
   }

   private void sendPrimaryAck(DataWriteCommand command, Object returnValue) throws Exception {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryAckCommand ackCommand = commandsFactory.buildPrimaryAckCommand();
      command.initPrimaryAck(ackCommand, returnValue);
      transport.noFcSendTo(origin, ackCommand, command.isSuccessful() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   private void sendBackupAck(DataWriteCommand command) throws Exception {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.backupAck(id.getId(), origin, command.getTopologyId());
      } else {
         transport.noFcSendTo(origin, commandsFactory.buildBackupAckCommand(id.getId(), command.getTopologyId()),
               DeliverOrder.NONE);
      }
   }

   private void sendPutMapBackupAck(PutMapCommand command, int segment) throws Exception {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.multiKeyBackupAck(id.getId(), localAddress, segment, command.getTopologyId());
      } else {
         transport.noFcSendTo(id.getAddress(),
               commandsFactory.buildBackupMultiKeyAckCommand(id.getId(), segment, command.getTopologyId()),
               DeliverOrder.NONE);
      }
   }

   private void sendPrimaryPutMapAck(PutMapCommand command,
         Map<Object, Object> returnValue) throws Exception {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryMultiKeyAckCommand ack = commandsFactory.buildPrimaryMultiKeyAckCommand(
            command.getCommandInvocationId().getId(),
            command.getTopologyId());
      if (command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
         ack.initWithoutReturnValue();
      } else {
         ack.initWithReturnValue(returnValue);
      }
      transport.noFcSendTo(id.getAddress(), ack, DeliverOrder.NONE);
   }

   private void sendExceptionAck(CommandInvocationId id, int topologyId, Throwable throwable) throws Exception {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id.getId(), throwable, topologyId);
      } else {
         transport.noFcSendTo(origin, commandsFactory.buildExceptionAckCommand(id.getId(), throwable, topologyId),
               DeliverOrder.NONE);
      }
   }
}
