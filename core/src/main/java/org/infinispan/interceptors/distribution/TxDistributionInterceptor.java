package org.infinispan.interceptors.distribution;

import static java.lang.String.format;
import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKeys;
import static org.infinispan.util.DeltaCompositeKeyUtil.getAffectedKeysFromContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.FunctionalCommand;
import org.infinispan.commands.functional.Mutation;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
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
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
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

   private final TxReadOnlyManyHelper txReadOnlyManyHelper = new TxReadOnlyManyHelper();
   private final ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper();
   private final ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper();

   private boolean syncRollbackPhase;

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Start
   public void start() {
      syncRollbackPhase = cacheConfiguration.transaction().syncRollbackPhase();
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

   @Override
   public BasicInvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleTxWriteManyEntriesCommand(ctx, command, command.getEntries(), (c, entries) -> new WriteOnlyManyEntriesCommand(c).withEntries(entries));
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleTxWriteManyCommand(ctx, command, command.getAffectedKeys(), (c, keys) -> new WriteOnlyManyCommand(c).withKeys(keys));
   }

   @Override
   public BasicInvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleFunctionalReadManyCommand(ctx, command, readWriteManyHelper);
      } else {
         return handleTxWriteManyCommand(ctx, command, command.getAffectedKeys(), readWriteManyHelper::copyForLocal);
      }
   }

   @Override
   public BasicInvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleFunctionalReadManyCommand(ctx, command, readWriteManyEntriesHelper);
      } else {
         return handleTxWriteManyEntriesCommand(ctx, command, command.getEntries(),
               (c, entries) -> new ReadWriteManyEntriesCommand<>(c).withEntries(entries));
      }
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

   protected <C extends TopologyAffectedCommand & FlagAffectedCommand, K, V> BasicInvocationStage
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

   protected <C extends VisitableCommand & FlagAffectedCommand, K> BasicInvocationStage handleTxWriteManyCommand(
         InvocationContext ctx, C command, Collection<K> keys, BiFunction<C, List<K>, C> copyCommand) {
         List<K> filtered = new ArrayList<>(keys.size());
      for (K key : keys) {
         if (ctx.isOriginLocal() || cdl.localNodeIsOwner(key)) {
            if (ctx.lookupEntry(key) == null) {
               entryFactory.wrapExternalEntry(ctx, key, null, true);
            }
            filtered.add(key);
         }
      }
      return invokeNext(ctx, copyCommand.apply(command, filtered));
   }

   public <C extends AbstractDataWriteCommand & FunctionalCommand> BasicInvocationStage handleTxFunctionalCommand(InvocationContext ctx, C command) {
      Object key = command.getKey();
      if (ctx.isOriginLocal()) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            if (isLocalModeForced(command) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
                  || command.loadType() == VisitableCommand.LoadType.DONT_LOAD) {
               entryFactory.wrapExternalEntry(ctx, key, null, true);
               return invokeNext(ctx, command);
            } else {
               CacheTopology cacheTopology = checkTopologyId(command);
               List<Address> owners = cacheTopology.getReadConsistentHash().locateOwners(command.getKey());

               List<Mutation> mutationsOnKey = getMutationsOnKey((TxInvocationContext) ctx, key);
               mutationsOnKey.add(command.toMutation(key));
               TxReadOnlyKeyCommand remoteRead = new TxReadOnlyKeyCommand(key, mutationsOnKey);

               return returnWithAsync(rpcManager.invokeRemotelyAsync(owners, remoteRead, staggeredOptions).thenApply(responses -> {
                  for (Response r : responses.values()) {
                     if (r instanceof SuccessfulResponse) {
                        SuccessfulResponse response = (SuccessfulResponse) r;
                        Object responseValue = response.getResponseValue();
                        return unwrapFunctionalResultOnOrigin(ctx, command.getKey(), responseValue);
                     }
                  }
                  // If this node has topology higher than some of the nodes and the nodes could not respond
                  // with the remote entry, these nodes are blocking the response and therefore we can get only timeouts.
                  // Therefore, if we got here it means that we have lower topology than some other nodes and we can wait
                  // for it in StateTransferInterceptor and retry the read later.
                  // TODO: These situations won't happen as soon as we'll implement 4-phase topology change in ISPN-5021
                  throw new OutdatedTopologyException("Did not get any successful response, got " + responses);
               }));
            }
         }
         // It's possible that this is not an owner, but the entry was loaded from L1 - let the command run
         return invokeNext(ctx, command);
      } else {
         if (!cdl.localNodeIsOwner(key)) {
            return returnWith(null);
         }
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            return handleMissingEntryOnRead(command);
         }
         return wrapFunctionalResultOnNonOriginOnReturn(invokeNext(ctx, command), entry);
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
      return createRpcOptionsFor2ndPhase(isSyncCommitPhase());
   }

   private RpcOptions createRollbackRpcOptions() {
      return createRpcOptionsFor2ndPhase(syncRollbackPhase);
   }

   private RpcOptions createRpcOptionsFor2ndPhase(boolean sync) {
      if (sync) {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      } else {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.NONE).build();
      }
   }

   protected RpcOptions createPrepareRpcOptions() {
      return defaultSynchronous ?
              rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build() :
              rpcManager.getDefaultRpcOptions(false);
   }

   @Override
   public BasicInvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleFunctionalReadManyCommand(ctx, command, txReadOnlyManyHelper);
   }

   @Override
   protected ReadOnlyKeyCommand remoteReadOnlyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) {
      if (!ctx.isInTxScope()) {
         return command;
      }
      return new TxReadOnlyKeyCommand(command, getMutationsOnKey((TxInvocationContext) ctx, command.getKey()));
   }

   private static List<Mutation> getMutationsOnKey(TxInvocationContext ctx, Object key) {
      TxInvocationContext txCtx = ctx;
      List<Mutation> mutations = new ArrayList<>();
      // We don't use getAllModifications() because this goes remote and local mods should not affect it
      for (WriteCommand write : txCtx.getCacheTransaction().getModifications()) {
         if (write.getAffectedKeys().contains(key)) {
            if (write instanceof FunctionalCommand) {
               mutations.add(((FunctionalCommand) write).toMutation(key));
            } else {
               // Non-functional modification must have retrieved the value into context and we should not do any
               // remote reads!
               throw new IllegalStateException("Attempt to remote functional read after non-functional modification! " +
                     "key=" + key + ", modification=" + write);
            }
         }
      }
      return mutations;
   }

   private static List<List<Mutation>> getMutations(InvocationContext ctx, List<Object> keys) {
      if (!ctx.isInTxScope()) {
         return null;
      }
      TxInvocationContext txCtx = (TxInvocationContext) ctx;
      List<List<Mutation>> mutations = new ArrayList<>(keys.size());
      for (int i = keys.size(); i > 0; --i) mutations.add(Collections.emptyList());

      for (WriteCommand write : txCtx.getCacheTransaction().getModifications()) {
         for (int i = 0; i < keys.size(); ++i) {
            Object key = keys.get(i);
            if (write.getAffectedKeys().contains(key)) {
               if (write instanceof FunctionalCommand) {
                  List<Mutation> list = mutations.get(i);
                  if (list.isEmpty()) {
                     list = new ArrayList<>();
                     mutations.set(i, list);
                  }
                  list.add(((FunctionalCommand) write).toMutation(key));
               } else {
                  // Non-functional modification must have retrieved the value into context and we should not do any
                  // remote reads!
                  throw new IllegalStateException("Attempt to remote functional read after non-functional modification! " +
                        "key=" + key + ", modification=" + write);
               }
            }
         }
      }
      return mutations;
   }

   private class TxReadOnlyManyHelper extends ReadOnlyManyHelper {
      @Override
      public ReplicableCommand copyForRemote(ReadOnlyManyCommand command, List<Object> keys, InvocationContext ctx) {
         List<List<Mutation>> mutations = getMutations(ctx, keys);
         if (mutations == null) {
            return new ReadOnlyManyCommand<>(command).withKeys(keys);
         } else {
            return new TxReadOnlyManyCommand(command, mutations).withKeys(keys);
         }
      }
   }

   private abstract class BaseFunctionalWriteHelper<C extends FunctionalCommand & WriteCommand> implements ReadManyCommandHelper<C> {
      @Override
      public Collection<?> keys(C command) {
         return command.getAffectedKeys();
      }

      @Override
      public ReplicableCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx) {
         List<List<Mutation>> mutations = getMutations(ctx, keys);
         // write command is always executed in transactional scope
         assert mutations != null;

         for (int i = 0; i < keys.size(); ++i) {
            List<Mutation> list = mutations.get(i);
            Mutation mutation = command.toMutation(keys.get(i));
            if (list.isEmpty()) {
               mutations.set(i, Collections.singletonList(mutation));
            } else {
               list.add(mutation);
            }
         }
         return new TxReadOnlyManyCommand(keys, mutations);
      }

      @Override
      public void applyLocalResult(MergingCompletableFuture allFuture, Object rv) {
         int pos = 0;
         for (Object value : ((List) rv)) {
            allFuture.results[pos++] = value;
         }
      }

      @Override
      public Object transformResult(Object[] results) {
         return Arrays.asList(results);
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         return wrapFunctionalManyResultOnNonOrigin(rCtx, ((WriteCommand) rCommand).getAffectedKeys(), ((List) rv).toArray());
      }
   }

   private class ReadWriteManyHelper extends BaseFunctionalWriteHelper<ReadWriteManyCommand> {
      @Override
      public ReadWriteManyCommand copyForLocal(ReadWriteManyCommand command, List<Object> keys) {
         return new ReadWriteManyCommand(command).withKeys(keys);
      }
   }

   private class ReadWriteManyEntriesHelper extends BaseFunctionalWriteHelper<ReadWriteManyEntriesCommand> {
      @Override
      public ReadWriteManyEntriesCommand copyForLocal(ReadWriteManyEntriesCommand command, List<Object> keys) {
         return new ReadWriteManyEntriesCommand(command).withEntries(filterEntries(command.getEntries(), keys));
      }

      private  <K, V> Map<K, V> filterEntries(Map<K, V> originalEntries, List<K> keys) {
         Map<K, V> entries = new HashMap<>(keys.size());
         for (K key : keys) {
            entries.put(key, originalEntries.get(key));
         }
         return entries;
      }
   }
}
