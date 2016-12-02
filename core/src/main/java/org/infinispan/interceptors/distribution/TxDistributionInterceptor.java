package org.infinispan.interceptors.distribution;

import static java.lang.String.format;
import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKeys;
import static org.infinispan.util.DeltaCompositeKeyUtil.getAffectedKeysFromContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Handles the distribution of the transactional caches.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 */
public class TxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(TxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private PartitionHandlingManager partitionHandlingManager;

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   private void updateMatcherForRetry(WriteCommand command) {
      // The command is already included in PrepareCommand.modifications - when the command is executed on the remote
      // owners it should not behave conditionally anymore because its success/failure is defined on originator.
      command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         return handleNonTxWriteCommand(ctx, command);
      }

      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleTxWriteManyEntriesCommand(ctx, command, command.getMap(),
            (c, entries) -> new PutMapCommand(c).withMap(entries));
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (ctx.isOriginLocal()) {
         TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) ctx;
         //In Pessimistic mode, the delta composite keys were sent to the wrong owner and never locked.
         final Collection<Address> affectedNodes = cdl.getOwners(filterDeltaCompositeKeys(command.getKeys()));
         localTxCtx.getCacheTransaction()
               .locksAcquired(affectedNodes == null ? dm.getConsistentHash().getMembers() : affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
               DeliverOrder.NONE).build();
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(affectedNodes, command, rpcOptions);
         return returnWithAsync(remoteInvocation.thenApply(responses -> {
            checkTxCommandResponses(responses, command, localTxCtx,
                  localTxCtx.getCacheTransaction().getRemoteLocksAcquired());
            return null;
         }));
      }
      return invokeNext(ctx, command);
   }

   // ---- TX boundary commands
   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createCommitRpcOptions());
         return returnWithAsync(remoteInvocation.thenApply(responses -> {
            checkTxCommandResponses(responses, command, ctx, recipients);
            return null;
         }));
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenCompose((stage, rCtx, rCommand, rv) -> {
         if (!shouldInvokeRemoteTxCommand(ctx)) {
            return returnWith(null);
         }

         TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) rCtx;
         Collection<Address> recipients = cdl.getOwners(getAffectedKeysFromContext(localTxCtx));
         CompletableFuture<Object> remotePrepare =
               prepareOnAffectedNodes(localTxCtx, (PrepareCommand) rCommand, recipients);
         return returnWithAsync(remotePrepare.thenApply(o -> {
            localTxCtx.getCacheTransaction().locksAcquired(
                  recipients == null ? dm.getWriteConsistentHash().getMembers() : recipients);
            return o;
         }));
      });
   }

   protected CompletableFuture<Object> prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command,
                                                              Collection<Address> recipients) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createPrepareRpcOptions());
         return remoteInvocation.handle((responses, t) -> {
            transactionRemotelyPrepared(ctx);
            CompletableFutures.rethrowException(t);

            checkTxCommandResponses(responses, command, (LocalTxInvocationContext) ctx, recipients);
            return null;
         });
      } catch (Throwable t) {
         transactionRemotelyPrepared(ctx);
         throw t;
      }
   }

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createRollbackRpcOptions());
         return returnWithAsync(remoteInvocation.thenApply(responses -> {
            checkTxCommandResponses(responses, command, ctx, recipients);
            return null;
         }));
      }

      return invokeNext(ctx, command);
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      Collection<Address> affectedNodes = cdl.getOwners(getAffectedKeysFromContext(ctx));
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   protected void checkTxCommandResponses(Map<Address, Response> responseMap,
         TransactionBoundaryCommand command, TxInvocationContext<LocalTransaction> context,
         Collection<Address> recipients) {
      OutdatedTopologyException outdatedTopologyException = null;
      for (Map.Entry<Address, Response> e : responseMap.entrySet()) {
         Address recipient = e.getKey();
         Response response = e.getValue();
         if (response == CacheNotFoundResponse.INSTANCE) {
            // No need to retry if the missing node wasn't a member when the command started.
            if (command.getTopologyId() == stateTransferManager.getCacheTopology().getTopologyId()
                  && !rpcManager.getMembers().contains(recipient)) {
               if (trace) log.tracef("Ignoring response from node not targeted %s", recipient);
            } else {
               if (checkCacheNotFoundResponseInPartitionHandling(command, context, recipients)) {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing. It will be handled by the PartitionHandlingManager", recipient);
                  return;
               } else {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing", recipient);
                  //noinspection ThrowableInstanceNeverThrown
                  outdatedTopologyException = new OutdatedTopologyException(format("Cache not running on node %s, or the node is missing", recipient));
               }
            }
         } else if (response == UnsureResponse.INSTANCE) {
            if (trace) log.tracef("Node %s has a newer topology id", recipient);
            //noinspection ThrowableInstanceNeverThrown
            outdatedTopologyException = new OutdatedTopologyException(format("Node %s has a newer topology id", recipient));
         }
      }
      if (outdatedTopologyException != null) {
         throw outdatedTopologyException;
      }
   }

   private boolean checkCacheNotFoundResponseInPartitionHandling(TransactionBoundaryCommand command,
         TxInvocationContext<LocalTransaction> context, Collection<Address> recipients) {
      final GlobalTransaction globalTransaction = command.getGlobalTransaction();
      final Collection<Object> lockedKeys = context.getLockedKeys();
      if (command instanceof RollbackCommand) {
         return partitionHandlingManager.addPartialRollbackTransaction(globalTransaction, recipients, lockedKeys);
      } else if (command instanceof PrepareCommand) {
         if (((PrepareCommand) command).isOnePhaseCommit()) {
            return partitionHandlingManager.addPartialCommit1PCTransaction(globalTransaction, recipients, lockedKeys,
                                                                           Arrays.asList(((PrepareCommand) command).getModifications()));
         }
      } else if (command instanceof CommitCommand) {
         EntryVersionsMap newVersion = null;
         if (command instanceof VersionedCommitCommand) {
            newVersion = ((VersionedCommitCommand) command).getUpdatedVersions();
         }
         return partitionHandlingManager.addPartialCommit2PCTransaction(globalTransaction, recipients, lockedKeys, newVersion);
      }
      return false;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private BasicInvocationStage handleTxWriteCommand(InvocationContext ctx, AbstractDataWriteCommand command,
         Object key) throws Throwable {
      try {
         if (!ctx.isOriginLocal() && !cdl.localNodeIsOwner(command.getKey())) {
            return returnWith(null);
         }
         CacheEntry entry = ctx.lookupEntry(command.getKey());
         if (entry == null) {
            if (isLocalModeForced(command) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP) || !needsPreviousValue(ctx, command)) {
               // in transactional mode, we always need the entry wrapped
               entryFactory.wrapExternalEntry(ctx, key, null, true);
            } else {
               // we need to retrieve the value locally regardless of load type; in transactional mode all operations
               // execute on origin
               // Also, operations that need value on backup [delta write] need to do the remote lookup even on non-origin
               return invokeNextAsync(ctx, command, remoteGet(ctx, command, command.getKey(), true)).handle(
                     (rCtx, rCommand, rv, t) -> updateMatcherForRetry((WriteCommand) rCommand));
            }
         }
         // already wrapped, we can continue
         return invokeNext(ctx, command).handle((rCtx, rCommand, rv, t) -> updateMatcherForRetry((WriteCommand) rCommand));
      } catch (Throwable t) {
         updateMatcherForRetry(command);
         throw t;
      }
   }

   protected <C extends AbstractTopologyAffectedCommand, K, V> BasicInvocationStage
         handleTxWriteManyEntriesCommand(InvocationContext ctx,C command, Map<K, V> entries,
                                  BiFunction<C, Map<K, V>, C> copyCommand) {
      Map<K, V> filtered = new HashMap<>(entries.size());
      Collection<CompletableFuture<?>> remoteGets = null;
      for (Map.Entry<K, V> e : entries.entrySet()) {
         K key = e.getKey();
         if (ctx.isOriginLocal() || cdl.localNodeIsOwner(key)) {
            if (ctx.lookupEntry(key) == null) {
               if (command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP) || !needsPreviousValue(ctx, command)) {
                  entryFactory.wrapExternalEntry(ctx, key, null, true);
               } else {
                  if (remoteGets == null) {
                     remoteGets = new ArrayList();
                  }
                  remoteGets.add(remoteGet(ctx, command, key, true));
               }
            }
            filtered.put(key, e.getValue());
         }
      }
      C narrowed = copyCommand.apply(command, filtered);
      if (remoteGets != null) {
         return invokeNextAsync(ctx, narrowed, CompletableFuture.allOf(remoteGets.toArray(new CompletableFuture[remoteGets.size()])));
      } else {
         return invokeNext(ctx, narrowed);
      }
   }

   private boolean needsPreviousValue(InvocationContext ctx, FlagAffectedCommand command) {
      switch (command.loadType()) {
         case DONT_LOAD:
            return false;
         case PRIMARY:
            // In transactional cache, the result is determined on origin
            return ctx.isOriginLocal();
         case OWNER:
            return true;
         default:
            throw new IllegalStateException();
      }
   }

   private RpcOptions createCommitRpcOptions() {
      return createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncCommitPhase());
   }

   private RpcOptions createRollbackRpcOptions() {
      return createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncRollbackPhase());
   }

   private RpcOptions createRpcOptionsFor2ndPhase(boolean sync) {
      if (sync) {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      } else {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.NONE).build();
      }
   }

   protected RpcOptions createPrepareRpcOptions() {
      return cacheConfiguration.clustering().cacheMode().isSynchronous() ?
              rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build() :
              rpcManager.getDefaultRpcOptions(false);
   }
}
