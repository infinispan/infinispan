package org.infinispan.interceptors.distribution;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
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
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.functional.EntryView;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.functional.impl.EntryViews;
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

   private static final Log log = LogFactory.getLog(TxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final long SKIP_REMOTE_FLAGS = FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP;

   private PartitionHandlingManager partitionHandlingManager;

   private final TxReadOnlyManyHelper txReadOnlyManyHelper = new TxReadOnlyManyHelper();
   private final ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper();
   private final ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper();

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      // Contrary to functional commands, compute() needs to return the new value returned by the remapping function.
      // Since we can assume that old and new values are comparable in size, fetching old value into context is acceptable
      // and it is more efficient in case that we execute more subsequent modifications than sending the return value.
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      // Contrary to functional commands, compute() needs to return the new value returned by the remapping function.
      // Since we can assume that old and new values are comparable in size, fetching old value into context is acceptable
      // and it is more efficient in case that we execute more subsequent modifications than sending the return value.
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   private void updateMatcherForRetry(WriteCommand command) {
      // The command is already included in PrepareCommand.modifications - when the command is executed on the remote
      // owners it should not behave conditionally anymore because its success/failure is defined on originator.
      command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         return handleNonTxWriteCommand(ctx, command);
      }

      return handleTxWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleTxWriteManyEntriesCommand(ctx, command, command.getMap(),
            (c, entries) -> new PutMapCommand(c).withMap(entries));
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (ctx.isOriginLocal()) {
         TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) ctx;
         Collection<Address> affectedNodes = dm.getCacheTopology().getWriteOwners(command.getKeys());
         Collection<Address> recipients = isReplicated ? null : affectedNodes;
         localTxCtx.getCacheTransaction().locksAcquired(affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
               DeliverOrder.NONE).build();
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, rpcOptions);
         return asyncValue(remoteInvocation.thenApply(responses -> {
            checkTxCommandResponses(responses, command, localTxCtx,
                  localTxCtx.getCacheTransaction().getRemoteLocksAcquired());
            return null;
         }));
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleTxWriteManyEntriesCommand(ctx, command, command.getEntries(), (c, entries) -> new WriteOnlyManyEntriesCommand(c).withEntries(entries));
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleTxFunctionalCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleTxWriteManyCommand(ctx, command, command.getAffectedKeys(), (c, keys) -> new WriteOnlyManyCommand(c).withKeys(keys));
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleFunctionalReadManyCommand(ctx, command, readWriteManyHelper);
      } else {
         return handleTxWriteManyCommand(ctx, command, command.getAffectedKeys(), readWriteManyHelper::copyForLocal);
      }
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleFunctionalReadManyCommand(ctx, command, readWriteManyEntriesHelper);
      } else {
         return handleTxWriteManyEntriesCommand(ctx, command, command.getEntries(),
               (c, entries) -> new ReadWriteManyEntriesCommand<>(c).withEntries(entries));
      }
   }

   // ---- TX boundary commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleSecondPhaseCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (!shouldInvokeRemoteTxCommand(ctx)) {
            return null;
         }

         TxInvocationContext<LocalTransaction> localTxCtx = (TxInvocationContext<LocalTransaction>) rCtx;
         LocalTransaction localTx = localTxCtx.getCacheTransaction();
         LocalizedCacheTopology cacheTopology =
               dm.getCacheTopology();
         Collection<Address> writeOwners = cacheTopology.getWriteOwners(localTxCtx.getAffectedKeys());
         localTx.locksAcquired(writeOwners);Collection<Address> recipients = isReplicated ? null : localTx.getCommitNodes(writeOwners, cacheTopology);
         CompletableFuture<Object> remotePrepare =
               prepareOnAffectedNodes(localTxCtx, (PrepareCommand) rCommand, recipients);
         return asyncValue(remotePrepare);
      });
   }

   protected CompletableFuture<Object> prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command,
                                                              Collection<Address> recipients) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createRpcOptions());
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
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleSecondPhaseCommand(ctx, command);
   }

   private Object handleSecondPhaseCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         CompletableFuture<Map<Address, Response>>
               remoteInvocation = rpcManager.invokeRemotelyAsync(recipients, command, createRpcOptions());
         return asyncValue(remoteInvocation.thenApply(responses -> {
            checkTxCommandResponses(responses, command, ctx, recipients);
            return null;
         }));
      }

      return invokeNext(ctx, command);
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      LocalizedCacheTopology cacheTopology = dm.getCacheTopology();
      Collection<Address> affectedNodes =
            isReplicated ? null : cacheTopology.getWriteOwners(ctx.getAffectedKeys());
      return localTx.getCommitNodes(affectedNodes, cacheTopology);
   }

   protected void checkTxCommandResponses(Map<Address, Response> responseMap,
         TransactionBoundaryCommand command, TxInvocationContext<LocalTransaction> context,
         Collection<Address> recipients) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      for (Map.Entry<Address, Response> e : responseMap.entrySet()) {
         Address recipient = e.getKey();
         Response response = e.getValue();
         if (response == CacheNotFoundResponse.INSTANCE) {
            // Prepare/Commit commands are sent to all affected nodes, including the ones that left the cluster.
            // We must not register a partial commit when receiving a CacheNotFoundResponse from one of those.
            if (!cacheTopology.getMembers().contains(recipient)) {
               if (trace) log.tracef("Ignoring response from node not targeted %s", recipient);
            } else {
               if (checkCacheNotFoundResponseInPartitionHandling(command, context, recipients)) {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing. It will be handled by the PartitionHandlingManager", recipient);
                  return;
               } else {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing", recipient);
                  throw OutdatedTopologyException.INSTANCE;
               }
            }
         } else if (response == UnsureResponse.INSTANCE) {
            if (trace) log.tracef("Node %s has a newer topology id", recipient);
            throw OutdatedTopologyException.INSTANCE;
         }
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
   private Object handleTxWriteCommand(InvocationContext ctx, AbstractDataWriteCommand command,
         Object key) throws Throwable {
      try {
         if (!ctx.isOriginLocal() && !dm.getCacheTopology().isWriteOwner(command.getKey())) {
            return null;
         }
         CacheEntry entry = ctx.lookupEntry(command.getKey());
         if (entry == null) {
            if (isLocalModeForced(command) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP) || !needsPreviousValue(ctx, command)) {
               // in transactional mode, we always need the entry wrapped
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            } else {
               // we need to retrieve the value locally regardless of load type; in transactional mode all operations
               // execute on origin
               // Also, operations that need value on backup [delta write] need to do the remote lookup even on non-origin
               Object result = asyncInvokeNext(ctx, command, remoteGet(ctx, command, command.getKey(), true));
               return makeStage(result)
                     .andFinally(ctx, command, (rCtx, rCommand, rv, t) ->
                           updateMatcherForRetry((WriteCommand) rCommand));
            }
         }
         // already wrapped, we can continue
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> updateMatcherForRetry((WriteCommand) rCommand));
      } catch (Throwable t) {
         updateMatcherForRetry(command);
         throw t;
      }
   }

   protected <C extends TopologyAffectedCommand & FlagAffectedCommand, K, V> Object
         handleTxWriteManyEntriesCommand(InvocationContext ctx, C command, Map<K, V> entries,
                                  BiFunction<C, Map<K, V>, C> copyCommand) {
      boolean ignorePreviousValue = command.hasAnyFlag(SKIP_REMOTE_FLAGS) || command.loadType() == VisitableCommand.LoadType.DONT_LOAD;
      Map<K, V> filtered = new HashMap<>(entries.size());
      Collection<CompletableFuture<?>> remoteGets = null;
      for (Map.Entry<K, V> e : entries.entrySet()) {
         K key = e.getKey();
         if (ctx.isOriginLocal() || dm.getCacheTopology().isWriteOwner(key)) {
            if (ctx.lookupEntry(key) == null) {
               if (ignorePreviousValue) {
                  entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               } else {
                  if (remoteGets == null) {
                     remoteGets = new ArrayList<>();
                  }
                  remoteGets.add(remoteGet(ctx, command, key, true));
               }
            }
            filtered.put(key, e.getValue());
         }
      }
      return asyncInvokeNext(ctx, copyCommand.apply(command, filtered), remoteGets);
   }

   protected <C extends VisitableCommand & FlagAffectedCommand & TopologyAffectedCommand, K> Object handleTxWriteManyCommand(
         InvocationContext ctx, C command, Collection<K> keys, BiFunction<C, List<K>, C> copyCommand) {
      boolean ignorePreviousValue = command.hasAnyFlag(SKIP_REMOTE_FLAGS) || command.loadType() == VisitableCommand.LoadType.DONT_LOAD;
      List<K> filtered = new ArrayList<>(keys.size());
      List<CompletableFuture<?>> remoteGets = null;
      for (K key : keys) {
         if (ctx.isOriginLocal() || dm.getCacheTopology().isWriteOwner(key)) {
            if (ctx.lookupEntry(key) == null) {
               if (ignorePreviousValue) {
                  entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               } else {
                  if (remoteGets == null) {
                     remoteGets = new ArrayList<>();
                  }
                  remoteGets.add(remoteGet(ctx, command, key, true));
               }
            }
            filtered.add(key);
         }
      }
      return asyncInvokeNext(ctx, copyCommand.apply(command, filtered), remoteGets);
   }

   public <C extends AbstractDataWriteCommand & FunctionalCommand> Object handleTxFunctionalCommand(InvocationContext ctx, C command) {
      Object key = command.getKey();
      if (ctx.isOriginLocal()) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            if (command.hasAnyFlag(SKIP_REMOTE_FLAGS)|| command.loadType() == VisitableCommand.LoadType.DONT_LOAD) {
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               return invokeNext(ctx, command);
            } else {
               LocalizedCacheTopology cacheTopology = checkTopologyId(command);
               Collection<Address> owners = cacheTopology.getDistribution(key).readOwners();

               List<Mutation> mutationsOnKey = getMutationsOnKey((TxInvocationContext) ctx, key);
               mutationsOnKey.add(command.toMutation(key));
               TxReadOnlyKeyCommand remoteRead = new TxReadOnlyKeyCommand(key, mutationsOnKey);

               return asyncValue(rpcManager.invokeRemotelyAsync(owners, remoteRead, getStaggeredOptions(owners.size())).thenApply(responses -> {
                  for (Response r : responses.values()) {
                     if (r instanceof SuccessfulResponse) {
                        SuccessfulResponse response = (SuccessfulResponse) r;
                        Object responseValue = response.getResponseValue();
                        return unwrapFunctionalResultOnOrigin(ctx, command.getKey(), responseValue);
                     }
                  }
                  throw handleMissingSuccessfulResponse(responses);
               }));
            }
         }
         // It's possible that this is not an owner, but the entry was loaded from L1 - let the command run
         return invokeNext(ctx, command);
      } else {
         if (!dm.getCacheTopology().isWriteOwner(key)) {
            return null;
         }
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            if (command.hasAnyFlag(SKIP_REMOTE_FLAGS) || command.loadType() == VisitableCommand.LoadType.DONT_LOAD) {
               // in transactional mode, we always need the entry wrapped
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            } else {
               return asyncInvokeNext(ctx, command, remoteGet(ctx, command, command.getKey(), true));
            }
         }
         return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
               wrapFunctionalResultOnNonOriginOnReturn(rv, entry));
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

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleFunctionalReadManyCommand(ctx, command, txReadOnlyManyHelper);
   }

   @Override
   protected ReadOnlyKeyCommand remoteReadOnlyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) {
      if (!ctx.isInTxScope()) {
         return command;
      }
      return new TxReadOnlyKeyCommand(command, getMutationsOnKey((TxInvocationContext) ctx, command.getKey()));
   }

   @Override
   protected <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletableFuture<Void> remoteGet(InvocationContext ctx, C command, Object key, boolean isWrite) {
      CompletableFuture<Void> cf = super.remoteGet(ctx, command, key, isWrite);
      // If the remoteGet is executed on non-origin node, the mutations list already contains all modifications
      // and we are just trying to execute all of them from EntryWrappingIntercepot$EntryWrappingVisitor
      if (!ctx.isOriginLocal() || !ctx.isInTxScope()) {
         return cf;
      }
      List<Mutation> mutationsOnKey = getMutationsOnKey((TxInvocationContext) ctx, key);
      if (mutationsOnKey.isEmpty()) {
         return cf;
      }
      return cf.thenRun(() -> {
         entryFactory.wrapEntryForWriting(ctx, key, false, true);
         MVCCEntry cacheEntry = (MVCCEntry) ctx.lookupEntry(key);
         EntryView.ReadWriteEntryView readWriteEntryView = EntryViews.readWrite(cacheEntry);
         for (Mutation mutation : mutationsOnKey) {
            mutation.apply(readWriteEntryView);
            cacheEntry.updatePreviousValue();
         }
      });
   }

   @Override
   protected void handleRemotelyRetrievedKeys(InvocationContext ctx, List<?> remoteKeys) {
      if (!ctx.isInTxScope()) {
         return;
      }
      List<List<Mutation>> mutations = getMutations(ctx, remoteKeys);
      if (mutations == null || mutations.isEmpty()) {
         return;
      }
      Iterator<?> keysIterator = remoteKeys.iterator();
      Iterator<List<Mutation>> mutationsIterator = mutations.iterator();
      for (; keysIterator.hasNext() && mutationsIterator.hasNext(); ) {
         Object key = keysIterator.next();
         entryFactory.wrapEntryForWriting(ctx, key, false, true);
         MVCCEntry cacheEntry = (MVCCEntry) ctx.lookupEntry(key);
         EntryView.ReadWriteEntryView readWriteEntryView = EntryViews.readWrite(cacheEntry);
         for (Mutation mutation : mutationsIterator.next()) {
            mutation.apply(readWriteEntryView);
            cacheEntry.updatePreviousValue();
         }
      }
      assert !keysIterator.hasNext();
      assert !mutationsIterator.hasNext();
   }

   protected RpcOptions createRpcOptions() {
      return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
   }

   private static List<Mutation> getMutationsOnKey(TxInvocationContext ctx, Object key) {
      List<Mutation> mutations = new ArrayList<>();
      // We don't use getAllModifications() because this goes remote and local mods should not affect it
      for (WriteCommand write : ctx.getCacheTransaction().getModifications()) {
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

   private static List<List<Mutation>> getMutations(InvocationContext ctx, List<?> keys) {
      if (!ctx.isInTxScope()) {
         return null;
      }
      log.tracef("Looking up mutations for %s", keys);
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
