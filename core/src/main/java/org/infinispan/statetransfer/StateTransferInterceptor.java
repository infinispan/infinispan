package org.infinispan.statetransfer;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
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
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.MissingOwnerException;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      // no need to forward this command
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitEvictCommand(InvocationContext ctx, EvictCommand command)
         throws Throwable {
      // it's not necessary to propagate eviction to the new owners in case of state transfer
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitReadCommand(ctx, command, NOP);
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return visitReadCommand(ctx, command, NOP);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return visitReadCommand(ctx, command, command::setConsistentHash);
   }

   private CompletableFuture<Void> visitReadCommand(InvocationContext ctx, FlagAffectedCommand command,
         Consumer<ConsistentHash> consistentHashUpdater) throws Throwable {
      // Remote reads coming from ClusteredGetCommand have local flag set, but we want to check them
//      if (isLocalOnly(command)) {
//         return ctx.continueInvocation();
//      }
      CacheTopology beginTopology = stateTransferManager.getCacheTopology();
      consistentHashUpdater.accept(beginTopology.getReadConsistentHash());
      updateTopologyId(command);
      return ctx.forkInvocation(command, (rCtx, rCommand, rv, throwable) -> {
         if (throwable == null)
            return rCtx.shortCircuit(rv);

         Throwable ce = throwable;
         while (ce instanceof RemoteException) {
            ce = ce.getCause();
         }
         if (ce instanceof OutdatedTopologyException || ce instanceof MissingOwnerException) {
            if (trace)
               log.tracef("Retrying command because of topology change, current topology is %d: %s",
                  currentTopologyId(), rCommand);
            // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the next topology.
            // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
            FlagAffectedCommand flagAffectedCommand = (FlagAffectedCommand) rCommand;
            int newTopologyId = Math.max(currentTopologyId(), flagAffectedCommand.getTopologyId() + 1);
            flagAffectedCommand.setTopologyId(newTopologyId);
            CompletableFuture<Void> future = stateTransferLock.topologyFuture(newTopologyId);
            if (future != null) {
               return future.thenCompose(ignored -> {
                  try {
                     return visitReadCommand(rCtx, flagAffectedCommand, consistentHashUpdater);
                  } catch (Throwable t) {
                     throw new CacheException(t);
                  }
               });
            }
         } else if (ce instanceof SuspectException) {
            if (trace)
               log.tracef("Retrying command because of suspected node, current topology is %d: %s",
                  currentTopologyId(), rCommand);
            // Upon SuspectException we don't wait for newer topology, as it is possible that current topology
            // is actual but the view still contains a node that's about to leave; a broadcast to all nodes
            // then can end with suspect exception, but we won't get any new topology. An example of this situation
            // is when a node sends leave - topology can be installed before the new view.
         } else {
            throw throwable;
         }
         return visitReadCommand(rCtx, (FlagAffectedCommand) rCommand, consistentHashUpdater);
      });
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx,
         ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return visitReadCommand(ctx, command, command::setConsistentHash);
   }

   /**
    * Special processing required for transaction commands.
    *
    */
   private CompletableFuture<Void> handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      // For local commands we may not have a GlobalTransaction yet
      Address origin = ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
      if (trace) log.tracef("handleTxCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return ctx.continueInvocation();
      }
      updateTopologyId(command);

      return ctx.forkInvocation(command, (rCtx, rCommand, rv, throwable) -> {
         Object localResult = rv;
         TransactionBoundaryCommand txCommand = (TransactionBoundaryCommand) rCommand;

         int retryTopologyId;
         if (throwable != null) {
            if (throwable instanceof OutdatedTopologyException || throwable instanceof MissingOwnerException) {
               // This can only happen on the originator
               retryTopologyId = Math.max(currentTopologyId(), txCommand.getTopologyId() + 1);
            } else {
               throw throwable;
            }
         } else {
            retryTopologyId = -1;
         }

         // We need to forward the command to the new owners, if the command was asynchronous
         boolean async = isTxCommandAsync(txCommand);
         if (async) {
            stateTransferManager.forwardCommandIfNeeded(txCommand, getAffectedKeys(rCtx, txCommand), origin);
            return rCtx.shortCircuit(rv);
         }

         if (rCtx.isOriginLocal()) {
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
               return retryCommandWithTransactionData(retryTopologyId, rCtx, txCommand,
                  (ctx2, cmd) -> handleTxCommand((TxInvocationContext) ctx2, cmd));
            }
         } else {
            if (currentTopologyId() > txCommand.getTopologyId()) {
               // Signal the originator to retry
               localResult = UnsureResponse.INSTANCE;
            }
         }
         return rCtx.shortCircuit(localResult);
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

   protected CompletableFuture<Void> handleWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (ctx.isInTxScope()) {
         return handleTxWriteCommand(ctx, command);
      } else {
         return handleNonTxWriteCommand(ctx, command);
      }
   }

   private CompletableFuture<Void> handleTxWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      Address origin = ctx.getOrigin();
      if (trace) log.tracef("handleTxWriteCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return ctx.continueInvocation();
      }
      updateTopologyId(command);

      return ctx.forkInvocation(command, (rCtx, rCommand, rv, throwable) -> {
         int retryTopologyId = -1;
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (throwable instanceof OutdatedTopologyException) {
            // This can only happen on the originator
            retryTopologyId = Math.max(currentTopologyId(), writeCommand.getTopologyId() + 1);
         } else if (throwable != null) {
            throw throwable;
         }

         if (rCtx.isOriginLocal()) {
            // On the originator, we only retry if we got an OutdatedTopologyException
            // Which could be caused either by an owner leaving or by an owner having a newer topology
            // No need to retry just because we have a new topology on the originator, all entries were
            // wrapped anyway
            if (retryTopologyId > 0) {
               // Only the originator can retry the command
               writeCommand.setTopologyId(retryTopologyId);
               return retryCommandWithTransactionData(retryTopologyId, rCtx, writeCommand,
                  (ctx2, cmd) -> handleTxWriteCommand(ctx2, cmd));
            }
         } else {
            if (currentTopologyId() > writeCommand.getTopologyId()) {
               // Signal the originator to retry
               return rCtx.shortCircuit(UnsureResponse.INSTANCE);
            }
         }
         return rCtx.shortCircuit(rv);
      });
   }

   /**
    * For non-tx write commands, we retry the command locally if the topology changed.
    * But we only retry on the originator, and only if the command doesn't have
    * the {@code CACHE_MODE_LOCAL} flag.
    */
   private CompletableFuture<Void> handleNonTxWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (trace) log.tracef("handleNonTxWriteCommand for command %s, topology id %d", command, command.getTopologyId());

      if (isLocalOnly(command)) {
         return ctx.continueInvocation();
      }

      updateTopologyId(command);

      // Only catch OutdatedTopologyExceptions on the originator
      if (!ctx.isOriginLocal()) {
         return ctx.continueInvocation();
      }

      return ctx.forkInvocation(command, (rCtx, rCommand, rv, throwable) -> {
         if (throwable == null)
            return rCtx.shortCircuit(rv);

         Throwable ce = throwable;
         while (ce instanceof RemoteException) {
            ce = ce.getCause();
         }
         if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException) && !(ce instanceof MissingOwnerException))
            throw throwable;

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
         int newTopologyId = Math.max(currentTopologyId, commandTopologyId + 1);
         writeCommand.setTopologyId(newTopologyId);
         writeCommand.addFlag(Flag.COMMAND_RETRY);
         return retryCommandWithTransactionData(newTopologyId, rCtx, writeCommand, (ctx2, cmd) -> handleNonTxWriteCommand(ctx2, cmd));
      });
   }

   @Override
   public CompletableFuture<Void> handleDefault(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin());
      } else {
         return ctx.continueInvocation();
      }
   }

   private CompletableFuture<Void> handleTopologyAffectedCommand(InvocationContext ctx,
         VisitableCommand command, Address origin) throws Throwable {
      if (trace) log.tracef("handleTopologyAffectedCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(command)) {
         return ctx.continueInvocation();
      }
      updateTopologyId((TopologyAffectedCommand) command);

      return ctx.continueInvocation();
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
