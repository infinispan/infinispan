package org.infinispan.statetransfer;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

//todo [anistor] command forwarding breaks the rule that we have only one originator for a command. this opens now the possibility to have two threads processing incoming remote commands for the same TX
/**
 * This interceptor has two tasks:
 * <ol>
 *    <li>If the command's topology id is higher than the current topology id,
 *    wait for the node to receive transaction data for the new topology id.</li>
 *    <li>If the topology id changed during a command's execution, retry the command, but only on the
 *    originator (which replicates it to the new owners).</li>
 * </ol>
 *
 * If the cache is configured with asynchronous replication, owners cannot signal to the originator that they
 * saw a new topology, so instead each owner forwards the command to all the other owners in the new topology.
 *
 * @author anistor@redhat.com
 */
public class StateTransferInterceptor extends BaseStateTransferInterceptor {

   private static final Log log = LogFactory.getLog(StateTransferInterceptor.class);
   private static boolean trace = log.isTraceEnabled();

   private StateTransferManager stateTransferManager;

   private boolean syncCommitPhase;
   private boolean defaultSynchronous;

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();
   private final InvocationComposeHandler handleReadCommandReturn = this::handleReadCommandReturn;
   private final InvocationComposeHandler handleTxReturn = this::handleTxReturn;
   private final InvocationComposeHandler handleTxWriteReturn = this::handleTxWriteReturn;
   private final InvocationComposeHandler handleNonTxWriteReturn = this::handleNonTxWriteReturn;

   @Inject
   public void init(StateTransferManager stateTransferManager) {
      this.stateTransferManager = stateTransferManager;
   }

   @Start
   public void start() {
      syncCommitPhase = cacheConfiguration.transaction().syncCommitPhase();
      defaultSynchronous = cacheConfiguration.clustering().cacheMode().isSynchronous();
   }

   @Override
   public InvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public InvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public InvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public InvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      // no need to forward this command
      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitEvictCommand(InvocationContext ctx, EvictCommand command)
         throws Throwable {
      // it's not necessary to propagate eviction to the new owners in case of state transfer
      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public InvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public InvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   private InvocationStage handleReadCommand(InvocationContext ctx, AbstractTopologyAffectedCommand command) throws Throwable {
      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);
      return invokeNext(ctx, command)
            .compose(ctx, command, handleReadCommandReturn);
   }

   private InvocationStage handleReadCommandReturn(InvocationStage stage, InvocationContext rCtx,
                                                   VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      if (t == null)
         return stage;

      Throwable ce = t;
      while (ce instanceof RemoteException) {
         ce = ce.getCause();
      }
      final CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      int currentTopologyId = cacheTopology == null ? -1 : cacheTopology.getTopologyId();
      AbstractTopologyAffectedCommand cmd = (AbstractTopologyAffectedCommand) rCommand;
      if (ce instanceof SuspectException) {
         if (trace)
            log.tracef("Retrying command because of suspected node, current topology is %d: %s",
                  currentTopologyId, rCommand);
         // It is possible that current topology is actual but the view still contains a node that's about to leave;
         // a broadcast to all nodes then can end with suspect exception, but we won't get any new topology.
         // An example of this situation is when a node sends leave - topology can be installed before the new view.
         // To prevent suspect exceptions use SYNCHRONOUS_IGNORE_LEAVERS response mode.
         if (currentTopologyId == cmd.getTopologyId() && !cacheTopology.getActualMembers().contains(((SuspectException) ce).getSuspect())) {
            // TODO: provide a test case
            throw new IllegalStateException("Command was not sent with SYNCHRONOUS_IGNORE_LEAVERS?");
         }
      } else if (ce instanceof OutdatedTopologyException) {
         if (trace)
            log.tracef("Retrying command because of topology change, current topology is %d: %s",
                  currentTopologyId, cmd);
      } else {
         return stage;
      }
      // We increment the topology to wait for the next topology.
      // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
      int newTopologyId = getNewTopologyId(ce, currentTopologyId, cmd);
      cmd.setTopologyId(newTopologyId);
      cmd.addFlags(FlagBitSets.COMMAND_RETRY);
      CompletableFuture<Void> topologyFuture = stateTransferLock.topologyFuture(newTopologyId);
      return retryWhenDone(topologyFuture, newTopologyId, rCtx, cmd)
            .compose(rCtx, rCommand, handleReadCommandReturn);
   }

   @Override
   public InvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx,
                                                        ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   /**
    * Special processing required for transaction commands.
    *
    */
   private InvocationStage handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      if (trace) log.tracef("handleTxCommand for command %s, origin %s", command, getOrigin(ctx));

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);

      return invokeNext(ctx, command)
            .compose(ctx, command, handleTxReturn);
   }

   private Address getOrigin(TxInvocationContext ctx) {
      // For local commands we may not have a GlobalTransaction yet
      return ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
   }

   private InvocationStage handleTxReturn(InvocationStage stage, InvocationContext ctx,
                                          VisitableCommand command, Object rv, Throwable t) throws Throwable {
      TransactionBoundaryCommand txCommand = (TransactionBoundaryCommand) command;

      int retryTopologyId = -1;
      int currentTopology = currentTopologyId();
      if (t instanceof OutdatedTopologyException) {
         // This can only happen on the originator
         retryTopologyId = Math.max(currentTopology, txCommand.getTopologyId() + 1);
      } else if (t != null) {
         return stage;
      }

      // We need to forward the command to the new owners, if the command was asynchronous
      boolean async = isTxCommandAsync(txCommand);
      if (async) {
         stateTransferManager.forwardCommandIfNeeded(txCommand, getAffectedKeys(ctx, txCommand), getOrigin((TxInvocationContext) ctx));
         return stage;
      }

      if (ctx.isOriginLocal()) {
         // On the originator, we only retry if we got an OutdatedTopologyException
         // Which could be caused either by an owner leaving or by an owner having a newer topology
         // No need to retry just because we have a new topology on the originator, all entries were
         // wrapped anyway

         if (retryTopologyId > 0) {
            // Only the originator can retry the command
            txCommand.setTopologyId(retryTopologyId);
            if (txCommand instanceof PrepareCommand) {
               ((PrepareCommand) txCommand).setRetriedCommand(true);
            }
            CompletableFuture<Void> transactionDataFuture = stateTransferLock.transactionDataFuture(retryTopologyId);
            return retryWhenDone(transactionDataFuture, retryTopologyId, ctx, txCommand)
                  .compose(ctx, command, handleTxReturn);
         }
      } else {
         if (currentTopology > txCommand.getTopologyId()) {
            // Signal the originator to retry
            return returnWith(UnsureResponse.INSTANCE);
         }
      }
      return stage;
   }

   private boolean isTxCommandAsync(TransactionBoundaryCommand command) {
      boolean async = false;
      if (command instanceof CommitCommand || command instanceof RollbackCommand) {
         async = !syncCommitPhase;
      } else if (command instanceof PrepareCommand) {
         async = !defaultSynchronous;
      }
      return async;
   }

   protected InvocationStage handleWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (ctx.isInTxScope()) {
         return handleTxWriteCommand(ctx, command);
      } else {
         return handleNonTxWriteCommand(ctx, command);
      }
   }

   private InvocationStage handleTxWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (trace) log.tracef("handleTxWriteCommand for command %s, origin %s", command, ctx.getOrigin());

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);

      return invokeNext(ctx, command)
            .compose(ctx, command, handleTxWriteReturn);
   }

   private InvocationStage handleTxWriteReturn(InvocationStage stage, InvocationContext rCtx,
                                               VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      int retryTopologyId = -1;
      WriteCommand writeCommand = (WriteCommand) rCommand;
      if (t instanceof OutdatedTopologyException) {
         // This can only happen on the originator
         retryTopologyId = Math.max(currentTopologyId(), writeCommand.getTopologyId() + 1);
      } else if (t != null) {
         throw t;
      }

      if (rCtx.isOriginLocal()) {
         // On the originator, we only retry if we got an OutdatedTopologyException
         // Which could be caused either by an owner leaving or by an owner having a newer topology
         // No need to retry just because we have a new topology on the originator, all entries were
         // wrapped anyway
         if (retryTopologyId > 0) {
            // Only the originator can retry the command
            writeCommand.setTopologyId(retryTopologyId);
            CompletableFuture<Void> transactionDataFuture = stateTransferLock.transactionDataFuture(retryTopologyId);
            return retryWhenDone(transactionDataFuture, retryTopologyId, rCtx, writeCommand)
                  .compose(rCtx, rCommand, handleTxWriteReturn);
         }
      } else {
         if (currentTopologyId() > writeCommand.getTopologyId()) {
            // Signal the originator to retry
            return returnWith(UnsureResponse.INSTANCE);
         }
      }
      return stage;
   }

   /**
    * For non-tx write commands, we retry the command locally if the topology changed.
    * But we only retry on the originator, and only if the command doesn't have
    * the {@code CACHE_MODE_LOCAL} flag.
    */
   private InvocationStage handleNonTxWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (trace) log.tracef("handleNonTxWriteCommand for command %s, topology id %d", command, command.getTopologyId());

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }

      updateTopologyId(command);

      // Only catch OutdatedTopologyExceptions on the originator
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }

      return invokeNext(ctx, command)
            .compose(ctx, command, handleNonTxWriteReturn);
   }

   private InvocationStage handleNonTxWriteReturn(InvocationStage stage, InvocationContext rCtx,
                                                  VisitableCommand rCommand, Object rv, Throwable t) throws Throwable {
      if (t == null)
         return stage;

      Throwable ce = t;
      while (ce instanceof RemoteException) {
         ce = ce.getCause();
      }
      if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException))
         throw t;

      // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the
      // next topology.
      // Without this, we could retry the command too fast and we could get the
      // OutdatedTopologyException again.
      int currentTopologyId = currentTopologyId();
      WriteCommand writeCommand = (WriteCommand) rCommand;
      if (trace)
         log.tracef("Retrying command because of topology change, current topology is %d: %s",
               currentTopologyId, writeCommand);
      int commandTopologyId = writeCommand.getTopologyId();
      int newTopologyId = getNewTopologyId(ce, currentTopologyId, writeCommand);
      writeCommand.setTopologyId(newTopologyId);
      writeCommand.addFlags(FlagBitSets.COMMAND_RETRY);
      // In non-tx context, waiting for transaction data is equal to waiting for topology
      CompletableFuture<Void> transactionDataFuture = stateTransferLock.transactionDataFuture(newTopologyId);
      return retryWhenDone(transactionDataFuture, newTopologyId, rCtx, writeCommand)
            .compose(rCtx, rCommand, handleNonTxWriteReturn);
   }

   private int getNewTopologyId(Throwable ce, int currentTopologyId, TopologyAffectedCommand command) {
      int requestedTopologyId = command.getTopologyId() + 1;
      if (ce instanceof OutdatedTopologyException) {
         OutdatedTopologyException ote = (OutdatedTopologyException) ce;
         if (ote.requestedTopologyId >= 0) {
            requestedTopologyId = ote.requestedTopologyId;
         }
      }
      return Math.max(currentTopologyId, requestedTopologyId);
   }

   @Override
   public InvocationStage handleDefault(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin());
      } else {
         return invokeNext(ctx, command);
      }
   }

   private InvocationStage handleTopologyAffectedCommand(InvocationContext ctx,
                                                         VisitableCommand command, Address origin) throws Throwable {
      if (trace) log.tracef("handleTopologyAffectedCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId((TopologyAffectedCommand) command);

      return invokeNext(ctx, command);
   }

   private boolean isLocalOnly(VisitableCommand command) {
      boolean cacheModeLocal = false;
      if (command instanceof FlagAffectedCommand) {
         cacheModeLocal = ((FlagAffectedCommand)command).hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
      }
      return cacheModeLocal;
   }

   @SuppressWarnings("unchecked")
   private Set<Object> getAffectedKeys(InvocationContext ctx, VisitableCommand command) {
      Set<Object> affectedKeys = null;
      try {
         affectedKeys = (Set<Object>) command.acceptVisitor(ctx, affectedKeysVisitor);
      } catch (Throwable throwable) {
         // impossible to reach this
      }
      if (affectedKeys == null) {
         affectedKeys = Collections.emptySet();
      }
      return affectedKeys;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
