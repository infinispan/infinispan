package org.infinispan.statetransfer;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.TimeoutException;
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
   public static final Consumer<ConsistentHash> NOP = ch -> {
   };

   private StateTransferManager stateTransferManager;

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();

   @Inject
   public void init(StateTransferManager stateTransferManager) {
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      // no need to forward this command
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitEvictCommand(InvocationContext ctx, EvictCommand command)
         throws Throwable {
      // it's not necessary to propagate eviction to the new owners in case of state transfer
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   private InvocationStage handleReadCommand(InvocationContext ctx, AbstractTopologyAffectedCommand command) throws Throwable {
      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);

      // Only catch OutdatedTopologyExceptions on the originator
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }

      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
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
         if (trace)
            log.tracef("Retrying command because of topology change, current topology is %d: %s",
                  currentTopologyId, rCommand);
         AbstractTopologyAffectedCommand cmd = (AbstractTopologyAffectedCommand) rCommand;
         int newTopologyId = Math.max(currentTopologyId, cmd.getTopologyId() + 1);
         cmd.setTopologyId(newTopologyId);
         waitForTopology(newTopologyId);

         return handleReadCommand(rCtx, cmd);
      });
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx,
         ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   /**
    * Special processing required for transaction commands.
    *
    */
   private BasicInvocationStage handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      // For local commands we may not have a GlobalTransaction yet
      Address origin = ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
      if (trace) log.tracef("handleTxCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);

      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
         Object localResult = rv;
         TransactionBoundaryCommand txCommand = (TransactionBoundaryCommand) rCommand;

         int retryTopologyId = -1;
         if (t instanceof OutdatedTopologyException) {
            // This can only happen on the originator
            retryTopologyId = Math.max(currentTopologyId(), txCommand.getTopologyId() + 1);
         } else if (t != null) {
            throw t;
         }

         // We need to forward the command to the new owners, if the command was asynchronous
         boolean async = isTxCommandAsync(txCommand);
         if (async) {
            stateTransferManager.forwardCommandIfNeeded(txCommand, getAffectedKeys(rCtx, txCommand), origin);
            return stage;
         }

         if (rCtx.isOriginLocal()) {
            // On the originator, we only retry if we got an OutdatedTopologyException
            // Which could be caused either by an owner leaving or by an owner having a newer topology
            // No need to retry just because we have a new topology on the originator, all entries were
            // wrapped anyway

            if (retryTopologyId > 0) {
               // Only the originator can retry the command
               txCommand.setTopologyId(retryTopologyId);
               waitForTransactionData(retryTopologyId);
               if (txCommand instanceof PrepareCommand) {
                  ((PrepareCommand) txCommand).setRetriedCommand(true);
               }

               log.tracef("Retrying command %s for topology %d", txCommand, retryTopologyId);
               return handleTxCommand((TxInvocationContext) rCtx, txCommand);
            }
         } else {
            if (currentTopologyId() > txCommand.getTopologyId()) {
               // Signal the originator to retry
               localResult = UnsureResponse.INSTANCE;
            }
         }
         return returnWith(localResult);
      });
   }

   private boolean isTxCommandAsync(TransactionBoundaryCommand command) {
      boolean async = false;
      if (command instanceof CommitCommand || command instanceof RollbackCommand) {
         async = !cacheConfiguration.transaction().syncCommitPhase();
      } else if (command instanceof PrepareCommand) {
         async = !cacheConfiguration.clustering().cacheMode().isSynchronous();
      }
      return async;
   }

   protected BasicInvocationStage handleWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (ctx.isInTxScope()) {
         return handleTxWriteCommand(ctx, command);
      } else {
         return handleNonTxWriteCommand(ctx, command);
      }
   }

   private BasicInvocationStage handleTxWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      Address origin = ctx.getOrigin();
      if (trace) log.tracef("handleTxWriteCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return invokeNext(ctx, command);
      }
      updateTopologyId(command);

      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
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
               waitForTransactionData(retryTopologyId);

               log.tracef("Retrying command %s for topology %d", writeCommand, retryTopologyId);
               return handleTxWriteCommand(rCtx, writeCommand);
            }
         } else {
            if (currentTopologyId() > writeCommand.getTopologyId()) {
               // Signal the originator to retry
               return returnWith(UnsureResponse.INSTANCE);
            }
         }
         return stage;
      });
   }

   /**
    * For non-tx write commands, we retry the command locally if the topology changed.
    * But we only retry on the originator, and only if the command doesn't have
    * the {@code CACHE_MODE_LOCAL} flag.
    */
   private BasicInvocationStage handleNonTxWriteCommand(InvocationContext ctx, WriteCommand command)
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

      return invokeNext(ctx, command).compose((stage, rCtx, rCommand, rv, t) -> {
         if (t == null)
            return stage;

         Throwable ce = t;
         while (ce instanceof RemoteException) {
            ce = ce.getCause();
         }
         if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException))
            throw t;
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (writeCommand.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT)) {
            // the command is expected to not wait
            TimeoutException timeoutException = new TimeoutException("Not waiting for new topology as this command has zero timeout set.");
            timeoutException.addSuppressed(t);
            throw timeoutException;
         }

         // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the
         // next topology.
         // Without this, we could retry the command too fast and we could get the
         // OutdatedTopologyException again.
         int currentTopologyId = currentTopologyId();
         if (trace)
            log.tracef("Retrying command because of topology change, current topology is %d: %s",
                  currentTopologyId, writeCommand);
         int commandTopologyId = writeCommand.getTopologyId();
         int newTopologyId = Math.max(currentTopologyId, commandTopologyId + 1);
         writeCommand.setTopologyId(newTopologyId);
         waitForTransactionData(newTopologyId);

         writeCommand.addFlag(Flag.COMMAND_RETRY);
         return handleNonTxWriteCommand(rCtx, writeCommand);
      });
   }

   @Override
   public BasicInvocationStage handleDefault(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin());
      } else {
         return invokeNext(ctx, command);
      }
   }

   private BasicInvocationStage handleTopologyAffectedCommand(InvocationContext ctx,
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
         cacheModeLocal = ((FlagAffectedCommand) command).hasFlag(Flag.CACHE_MODE_LOCAL);
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
