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
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
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

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CommandAckCollector commandAckCollector;
   private DistributionManager distributionManager;
   private StateTransferManager stateTransferManager;
   private Address localAddress;

   private final InvocationFinallyFunction onLocalWriteCommand = this::onLocalWriteCommand;
   private final InvocationFinallyAction onRemotePrimaryOwner = this::onRemotePrimaryOwner;
   private final InvocationFinallyAction onRemoteBackupOwner = this::onRemoteBackupOwner;

   @Inject
   public void inject(RpcManager rpcManager, CommandsFactory commandsFactory, CommandAckCollector commandAckCollector,
         DistributionManager distributionManager, StateTransferManager stateTransferManager) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.commandAckCollector = commandAckCollector;
      this.distributionManager = distributionManager;
      this.stateTransferManager = stateTransferManager;
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
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
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command);
      } else {
         return handleRemotePutMapCommand(ctx, command);
      }
   }

   private Object handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (t != null) {
            sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), t);
            throw t;
         }
         int segment = distributionManager.getConsistentHash().getSegment(command.getMap().keySet().iterator().next());
         if (cmd.isForwarded()) {
            sendPutMapBackupAck(cmd, segment);
         } else {
            //noinspection unchecked
            sendPrimaryPutMapAck(cmd, (Map<Object, Object>) rv);
         }
         return rv;
      });
   }

   private Object handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         final PutMapCommand cmd = (PutMapCommand) rCommand;
         if (throwable != null) {
            disposeCollectorOnException(cmd.getCommandInvocationId());
            throw throwable;
         }
         return waitCollectorAsync(cmd.getCommandInvocationId(), rv);
      });
   }


   private Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (ctx.isOriginLocal()) {
         return invokeNextAndHandle(ctx, command, onLocalWriteCommand);
      } else {
         CacheTopology cacheTopology = checkTopologyId(command);
         DistributionInfo distributionInfo = new DistributionInfo(command.getKey(),
               cacheTopology.getWriteConsistentHash(), localAddress);
         switch (distributionInfo.ownership()) {
            case BACKUP:
               return invokeNextAndFinally(ctx, command, onRemoteBackupOwner);
            case PRIMARY:
               return invokeNextAndFinally(ctx, command, onRemotePrimaryOwner);
            default:
               throw new IllegalStateException();
         }
      }
   }

   private CacheTopology checkTopologyId(DataWriteCommand command) {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      if (command.getTopologyId() != cacheTopology.getTopologyId()) {
         sendExceptionAck(command.getCommandInvocationId(), command.getTopologyId(),
               OutdatedTopologyException.getCachedInstance());
         throw OutdatedTopologyException.getCachedInstance();
      }
      return cacheTopology;
   }

   @SuppressWarnings("unused")
   private void onRemotePrimaryOwner(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) {
      DataWriteCommand command = (DataWriteCommand) rCommand;
      if (throwable != null) {
         sendExceptionAck(command.getCommandInvocationId(), command.getTopologyId(), throwable);
      } else {
         sendPrimaryAck(command, rv);
      }
   }

   @SuppressWarnings("unused")
   private void onRemoteBackupOwner(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable) {
      DataWriteCommand cmd = (DataWriteCommand) rCommand;
      if (throwable != null) {
         sendExceptionAck(cmd.getCommandInvocationId(), cmd.getTopologyId(), throwable);
      } else {
         sendBackupAck(cmd);
      }
   }

   @SuppressWarnings("unused")
   private Object onLocalWriteCommand(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable)
         throws Throwable {
      final DataWriteCommand cmd = (DataWriteCommand) rCommand;
      if (throwable != null) {
         disposeCollectorOnException(cmd.getCommandInvocationId());
         throw throwable;
      }
      return waitCollectorAsync(cmd.getCommandInvocationId(), rv);
   }

   private void disposeCollectorOnException(CommandInvocationId id) {
      //a local exception occur. No need to wait for acknowledges.
      commandAckCollector.dispose(id);
   }

   private Object waitCollectorAsync(CommandInvocationId id, Object rv) {
      //waiting for acknowledges based on default rpc timeout.
      CompletableFuture<Object> collectorFuture = commandAckCollector.getCollectorCompletableFutureToWait(id);
      if (collectorFuture == null) {
         //no collector, return immediately.
         return rv;
      }
      return asyncValue(collectorFuture);
   }

   private void sendPrimaryAck(DataWriteCommand command, Object returnValue) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryAckCommand ackCommand = commandsFactory.buildPrimaryAckCommand();
      command.initPrimaryAck(ackCommand, returnValue);
      rpcManager.sendTo(origin, ackCommand, command.isSuccessful() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   private void sendBackupAck(DataWriteCommand command) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.backupAck(id, origin, command.getTopologyId());
      } else {
         rpcManager
               .sendTo(origin, commandsFactory.buildBackupAckCommand(id, command.getTopologyId()), DeliverOrder.NONE);
      }
   }

   private void sendPutMapBackupAck(PutMapCommand command, int segment) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      if (id.getAddress().equals(localAddress)) {
         commandAckCollector.multiKeyBackupAck(id, localAddress, segment, command.getTopologyId());
      } else {
         rpcManager.sendTo(id.getAddress(),
               commandsFactory.buildBackupMultiKeyAckCommand(id, segment, command.getTopologyId()), DeliverOrder.NONE);
      }
   }

   private void sendPrimaryPutMapAck(PutMapCommand command,
         Map<Object, Object> returnValue) {
      final CommandInvocationId id = command.getCommandInvocationId();
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending ack for command %s. Originator=%s.", id, origin);
      }
      PrimaryMultiKeyAckCommand ack = commandsFactory.buildPrimaryMultiKeyAckCommand(
            command.getCommandInvocationId(),
            command.getTopologyId());
      if (command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
         ack.initWithoutReturnValue();
      } else {
         ack.initWithReturnValue(returnValue);
      }
      rpcManager.sendTo(id.getAddress(), ack, DeliverOrder.NONE);
   }

   private void sendExceptionAck(CommandInvocationId id, int topologyId, Throwable throwable) {
      final Address origin = id.getAddress();
      if (trace) {
         log.tracef("Sending exception ack for command %s. Originator=%s.", id, origin);
      }
      if (origin.equals(localAddress)) {
         commandAckCollector.completeExceptionally(id, throwable, topologyId);
      } else {
         rpcManager
               .sendTo(origin, commandsFactory.buildExceptionAckCommand(id, throwable, topologyId), DeliverOrder.NONE);
      }
   }
}
