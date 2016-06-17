package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKeys;
import static org.infinispan.util.DeltaCompositeKeyUtil.getAffectedKeysFromContext;

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

   private boolean useClusteredWriteSkewCheck;

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Start
   public void start() {
      useClusteredWriteSkewCheck = Configurations.isVersioningEnabled(cacheConfiguration);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      if (ctx.isOriginLocal()) {
         ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            // If the state transfer interceptor has to retry the command, it should ignore the previous
            // value.
            ReplaceCommand replaceCommand = (ReplaceCommand) rCommand;
            replaceCommand.setValueMatcher(
                  replaceCommand.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
            return null;
         });
      }
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command)
         throws Throwable {
      if (ctx.isOriginLocal()) {
         ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            // If the state transfer interceptor has to retry the command, it should ignore the previous
            // value.
            RemoveCommand removeCommand = (RemoveCommand) rCommand;
            removeCommand.setValueMatcher(
                  removeCommand.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
            return null;
         });
      }
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         return handleNonTxWriteCommand(ctx, command);
      }

      if (ctx.isOriginLocal()) {
         ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            // If the state transfer interceptor has to retry the command, it should ignore the previous
            // value.
            PutKeyValueCommand putKeyValueCommand = (PutKeyValueCommand) rCommand;
            putKeyValueCommand.setValueMatcher(
                  putKeyValueCommand.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
            return null;
         });
      }
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitGetCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitGetCommand(ctx, command);
   }

   private CompletableFuture<Void> visitGetCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      // If the cache entry has the value lock flag set, skip the remote get.
      if (ctx.isOriginLocal() && valueIsMissing(entry)) {
         if (readNeedsRemoteValue(ctx, command)) {
            remoteGet(ctx, key, false, command);
         }
      }

      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) ctx;
         //In Pessimistic mode, the delta composite keys were sent to the wrong owner and never locked.
         final Collection<Address> affectedNodes = cdl.getOwners(filterDeltaCompositeKeys(command.getKeys()));
         localTxCtx.getCacheTransaction()
               .locksAcquired(affectedNodes == null ? dm.getConsistentHash().getMembers() : affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(affectedNodes, command, rpcOptions);
         checkTxCommandResponses(responseMap, command, localTxCtx,
               localTxCtx.getCacheTransaction().getRemoteLocksAcquired());
      }
      return ctx.continueInvocation();
   }

   // ---- TX boundary commands
   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         Map<Address, Response> responseMap =
               rpcManager.invokeRemotely(recipients, command, createCommitRpcOptions());
         checkTxCommandResponses(responseMap, command, ctx, recipients);
      }
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command)
         throws Throwable {
      if (!ctx.isOriginLocal()) {
         return ctx.continueInvocation();
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null && shouldInvokeRemoteTxCommand(ctx)) {
            TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) rCtx;
            Collection<Address> recipients = cdl.getOwners(getAffectedKeysFromContext(localTxCtx));
            prepareOnAffectedNodes(localTxCtx, (PrepareCommand) rCommand, recipients);
            localTxCtx.getCacheTransaction().locksAcquired(
                  recipients == null ? dm.getWriteConsistentHash().getMembers() : recipients);
         }
         return null;
      });
   }

   protected void prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, createPrepareRpcOptions());
         checkTxCommandResponses(responseMap, command, (LocalTxInvocationContext) ctx, recipients);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, createRollbackRpcOptions());
         checkTxCommandResponses(responseMap, command, ctx, recipients);
      }

      return ctx.continueInvocation();
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
   private CompletableFuture<Void> handleTxWriteCommand(InvocationContext ctx, WriteCommand command,
         Object key) throws Throwable {
      // see if we need to load values from remote sources first
      return remoteGetBeforeWrite(ctx, command, key);
   }

   @Override
   protected boolean writeNeedsRemoteValue(InvocationContext ctx, WriteCommand command, Object key) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         return false;
      }
      if (ctx.isOriginLocal()) {
         // The return value only matters on the originator.
         // Conditional commands also check the previous value only on the originator.
         if (!command.readsExistingValues()) {
            return false;
         }
         // TODO Could make DELTA_WRITE/ApplyDeltaCommand override SKIP_REMOTE_LOOKUP by changing next line to
         // return !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP) || command.alwaysReadsExistingValues();
         return !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP);
      } else {
         // Ignore SKIP_REMOTE_LOOKUP on remote nodes
         // TODO Can we ignore the CACHE_MODE_LOCAL flag as well?
         return command.alwaysReadsExistingValues();
      }
   }

   @Override
   protected CompletableFuture<Void> remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command,
         Object key) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (!valueIsMissing(entry)) {
         // The entry already exists in the context, and it shouldn't be re-fetched
         return ctx.continueInvocation();
      }
      if (writeNeedsRemoteValue(ctx, command, key)) {
         remoteGet(ctx, key, true, command);
      }
      return ctx.continueInvocation();
   }

   protected InternalCacheEntry remoteGet(InvocationContext ctx, Object key, boolean isWrite,
                                          FlagAffectedCommand command) throws Throwable {
      // attempt a remote lookup
      InternalCacheEntry ice = retrieveFromProperSource(key, ctx, false, command, isWrite).get();

      if (ice != null) {
         if (useClusteredWriteSkewCheck && ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).getCacheTransaction().putLookedUpRemoteVersion(key, ice.getMetadata().version());
         }

         EntryFactory.Wrap wrap = isWrite ? EntryFactory.Wrap.WRAP_NON_NULL : EntryFactory.Wrap.STORE;
         entryFactory.wrapExternalEntry(ctx, key, ice, wrap, false);
         return ice;
      }
      return null;
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
