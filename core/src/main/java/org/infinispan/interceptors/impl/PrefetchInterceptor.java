package org.infinispan.interceptors.impl;

import static org.infinispan.context.impl.FlagBitSets.SKIP_OWNERSHIP_CHECK;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheSet;
import org.infinispan.InternalCacheSet;
import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class PrefetchInterceptor<K, V> extends DDAsyncInterceptor {
   protected static final Log log = LogFactory.getLog(PrefetchInterceptor.class);

   protected static final long STATE_TRANSFER_FLAGS = FlagBitSets.PUT_FOR_STATE_TRANSFER |
         FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP |
         FlagBitSets.SKIP_SHARED_CACHE_STORE | SKIP_OWNERSHIP_CHECK | FlagBitSets.SKIP_XSITE_BACKUP;
   @Inject protected ScatteredVersionManager<K> svm;
   @Inject protected DistributionManager dm;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected RpcManager rpcManager;
   @Inject protected ComponentRef<AdvancedCache<K, V>> cache;
   @Inject protected EntryFactory entryFactory;
   @Inject protected InternalDataContainer<K, V> dataContainer;

   protected int numSegments;

   private final InvocationSuccessFunction<VisitableCommand> handleRemotelyPrefetchedEntry = this::handleRemotelyPrefetchedEntry;

   @Start
   public void start() {
      this.numSegments = cacheConfiguration.clustering().hash().numSegments();
   }

   private boolean canRetrieveRemoteValue(FlagAffectedCommand command) {
      // We have to do prefetch for remote-originating reads as these have local flag set
      return !command.hasAnyFlag(SKIP_OWNERSHIP_CHECK);
   }

   protected Object handleReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ctx.removeLookedUpEntry(command.getKey());
      }
      if (canRetrieveRemoteValue(command)) {
         return prefetchKeyIfNeededAndInvokeNext(ctx, command, command.getKey(), false);
      } else {
         return invokeNext(ctx, command);
      }
   }

   private Object prefetchKeyIfNeededAndInvokeNext(InvocationContext ctx, DataCommand command, Object key, boolean isWrite) {
      int segment = command.getSegment();
      switch (svm.getSegmentState(segment)) {
         case NOT_OWNED:
            // if we're not a primary owner of this segment, we don't have to wait for it
            break;
         case BLOCKED:
            if (isWrite) {
               return asyncValue(svm.getBlockingFuture(segment))
                     .thenApply(ctx, command, (rCtx, rCommand, ignored) ->
                           prefetchKeyIfNeededAndInvokeNext(ctx, command, key, true));
            }
         case KEY_TRANSFER:
         case VALUE_TRANSFER:
            InvocationStage nextStage = lookupLocalAndRetrieveRemote(ctx, key, command, segment);
            if (nextStage != null) {
               return asyncInvokeNext(ctx, command, nextStage);
            }
            break;
         case OWNED:
            break;
         default:
            throw new IllegalStateException();
      }
      return invokeNext(ctx, command);
   }

   private <C extends VisitableCommand & TopologyAffectedCommand> Object prefetchKeysIfNeededAndInvokeNext(
         InvocationContext ctx, C command, Collection<?> keys, boolean isWrite) {
      BitSet blockedSegments = null;
      List<Object> transferedKeys = null;
      for (Object key : keys) {
         int segment = keyPartitioner.getSegment(key);
         switch (svm.getSegmentState(segment)) {
            case NOT_OWNED:
               // if we're not a primary owner of this segment, we don't have to wait for it
               break;
            case BLOCKED:
               if (isWrite) {
                  if (blockedSegments == null) {
                     blockedSegments = new BitSet(numSegments);
                  }
                  blockedSegments.set(segment);
               }
            case KEY_TRANSFER:
            case VALUE_TRANSFER:
               if (transferedKeys == null) {
                  transferedKeys = new ArrayList<>(keys.size());
               }
               transferedKeys.add(key);
            case OWNED:
               break;
            default:
               throw new IllegalStateException();
         }
      }
      if (blockedSegments != null) {
         CompletableFuture<Void> blockingFuture = CompletableFuture.allOf(blockedSegments.stream()
               .mapToObj(svm::getBlockingFuture).toArray(CompletableFuture[]::new));
         return asyncValue(blockingFuture).thenApply(ctx, command,
               // Make sure command is passed to prefetchKeys - it is already a capturing lambda by referencing
               // keys. This confuses graal otherwise
               (rCtx, rCommand, rv) -> prefetchKeysIfNeededAndInvokeNext(rCtx, rCommand, keys, true));
      }
      if (transferedKeys != null) {
         return asyncInvokeNext(ctx, command, retrieveRemoteValues(ctx, command, transferedKeys));
      } else {
         return invokeNext(ctx, command);
      }
   }

   private InvocationStage lookupLocalAndRetrieveRemote(InvocationContext ctx, Object key, DataCommand cmd, int segment) {
      // We need to lookup the dataContainer directly as GetCacheEntryCommand won't return entry with null value
      InternalCacheEntry<K, V> entry = dataContainer.peek(segment, key);
      if (log.isTraceEnabled()) {
         log.tracef("Locally prefetched entry %s", entry);
      }
      Metadata metadata = entry != null ? entry.getMetadata() : null;
      if (metadata != null && metadata.version() != null && svm.isVersionActual(segment, metadata.version())) {
         entryFactory.wrapExternalEntry(ctx, key, entry, true, true);
         return null;
      } else if ((metadata instanceof RemoteMetadata) &&
            svm.getSegmentState(segment) == ScatteredVersionManager.SegmentState.VALUE_TRANSFER) {
         // The RemoteMetadata is valid only during value transfer - in blocked state there shouldn't be any such
         // entry and during key transfer we could see metadata pointing to a node with outdated information.
         Address backup = ((RemoteMetadata) metadata).getAddress();
         return retrieveRemoteValue(ctx, Collections.singleton(backup), key, segment, cmd);
      } else {
         return retrieveRemoteValue(ctx, null, key, segment, cmd);
      }
   }

   private InvocationStage retrieveRemoteValue(InvocationContext ctx, Collection<Address> targets, Object key, int segment,
                                                                   DataCommand dataCommand) {
      if (log.isTraceEnabled()) {
         log.tracef("Prefetching entry for key %s from %s", key, targets);
      }
      ClusteredGetCommand command = commandsFactory.buildClusteredGetCommand(key, segment, FlagBitSets.SKIP_OWNERSHIP_CHECK);
      command.setTopologyId(dataCommand.getTopologyId());
      CompletionStage<Map<Address, Response>> remoteInvocation =
            targets != null ?
            rpcManager.invokeCommand(targets, command, MapResponseCollector.ignoreLeavers(targets.size()),
                                     rpcManager.getSyncRpcOptions()) :
            rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(),
                                          rpcManager.getSyncRpcOptions());
      return asyncValue(remoteInvocation).thenApplyMakeStage(ctx, dataCommand, handleRemotelyPrefetchedEntry);
   }

   private Object handleRemotelyPrefetchedEntry(InvocationContext ctx, VisitableCommand command, Object rv) {
      Map<Address, Response> responseMap = (Map<Address, Response>) rv;
      EntryVersion maxVersion = null;
      InternalCacheValue<V> maxValue = null;
      for (Response response : responseMap.values()) {
         if (!response.isSuccessful()) {
            throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
         }
         SuccessfulResponse<InternalCacheValue<V>> successfulResponse = (SuccessfulResponse<InternalCacheValue<V>>) response;
         InternalCacheValue<V> icv = successfulResponse.getResponseValue();
         if (icv == null) {
            continue;
         }
         Metadata metadata = icv.getMetadata();
         if (metadata instanceof RemoteMetadata) {
            // Clustered get is sent with SKIP_OWNERSHIP_CHECK and that means that the topology won't be checked.
            // PrefetchInterceptor on the remote node won't try to fetch the value either, so retrieving remote value
            // from another node is possible.
            throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
         }
         if (metadata != null && metadata.version() != null) {
            if (maxVersion == null || maxVersion.compareTo(metadata.version()) == InequalVersionComparisonResult.BEFORE) {
               maxVersion = metadata.version();
               maxValue = icv;
            }
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Prefetched value is %s", maxValue);
      }
      DataCommand dataCommand = (DataCommand) command;
      if (maxValue == null) {
         return null;
      }
      // The put below could fail updating the context if the data container got the updated value while we were
      // prefetching that (then the version would not be higher than the on in DC).
      // We need to call RepeatableReadEntry.updatePreviousValue() (through wrapExternalEntry) to get return value
      // from the main comman d correct.
      entryFactory.wrapExternalEntry(ctx, dataCommand.getKey(), maxValue.toInternalCacheEntry(dataCommand.getKey()), true, true);
      PutKeyValueCommand putKeyValueCommand = commandsFactory.buildPutKeyValueCommand(
         dataCommand.getKey(), maxValue.getValue(), dataCommand.getSegment(), new InternalMetadataImpl(maxValue), STATE_TRANSFER_FLAGS);
      putKeyValueCommand.setTopologyId(dataCommand.getTopologyId());
      return invokeNext(ctx, putKeyValueCommand);
   }

   // TODO: this is not completely aligned with single-entry prefetch
   private <C extends VisitableCommand & TopologyAffectedCommand> InvocationStage retrieveRemoteValues(InvocationContext ctx, C originCommand, List<?> keys) {
      if (log.isTraceEnabled()) {
         log.tracef("Prefetching entries for keys %s using broadcast", keys);
      }
      ClusteredGetAllCommand<?, ?> command = commandsFactory.buildClusteredGetAllCommand(keys, FlagBitSets.SKIP_OWNERSHIP_CHECK, null);
      command.setTopologyId(originCommand.getTopologyId());
      CompletionStage<Map<Address, Response>> rpcFuture = rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      return asyncValue(rpcFuture).thenApplyMakeStage(ctx, originCommand, (rCtx, topologyAffectedCommand, rv) -> {
         Map<Address, Response> responseMap = (Map<Address, Response>) rv;
         InternalCacheValue<V>[] maxValues = new InternalCacheValue[keys.size()];
         for (Response response : responseMap.values()) {
            if (!response.isSuccessful()) {
               throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
            }
            InternalCacheValue<V>[] values = ((SuccessfulResponse<InternalCacheValue<V>[]>) response).getResponseValue();
            int i = 0;
            for (InternalCacheValue<V> icv : values) {
               if (icv != null) {
                  Metadata metadata = icv.getMetadata();
                  if (metadata instanceof RemoteMetadata) {
                     // not sure if this can happen, but let's be on the safe side
                     throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
                  }
                  if (maxValues[i] == null) {
                     maxValues[i] = icv;
                  } else if (metadata != null && metadata.version() != null) {
                     Metadata maxMetadata;
                     if ((maxMetadata = maxValues[i].getMetadata()) == null || maxMetadata.version() == null
                        || maxMetadata.version().compareTo(metadata.version()) == InequalVersionComparisonResult.BEFORE) {
                        maxValues[i] = icv;
                     }
                  }
               }
               ++i;
            }
         }
         Map<Object, InternalCacheValue<V>> map = new HashMap<>(keys.size());
         for (int i = 0; i < maxValues.length; ++i) {
            if (maxValues[i] != null) {
               map.put(keys.get(i), maxValues[i]);
            }
         }
         if (log.isTraceEnabled()) {
            log.tracef("Prefetched values are %s", map);
         }
         if (map.isEmpty()) {
            return CompletableFutures.completedNull();
         }
         // The put below could fail updating the context if the data container got the updated value while we were
         // prefetching that. Also, we need to call RepeatableReadEntry.updatePreviousValue() to get return value
         // from the main command correct.
         for (Map.Entry<Object, InternalCacheValue<V>> entry : map.entrySet()) {
            entryFactory.wrapExternalEntry(rCtx, entry.getKey(), entry.getValue().toInternalCacheEntry(entry.getKey()), true, true);
         }
         PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
         putMapCommand.setTopologyId(topologyAffectedCommand.getTopologyId());
         return invokeNext(rCtx, putMapCommand);
      });
   }

   protected Object handleReadManyCommand(InvocationContext ctx, AbstractTopologyAffectedCommand command, Collection<?> keys) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ctx.removeLookedUpEntries(keys);
      }
      if (canRetrieveRemoteValue(command)) {
         return prefetchKeysIfNeededAndInvokeNext(ctx, command, keys, false);
      } else {
         return invokeNext(ctx, command);
      }
   }

   protected Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ctx.removeLookedUpEntry(command.getKey());
      }
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD && canRetrieveRemoteValue(command)) {
         return prefetchKeyIfNeededAndInvokeNext(ctx, command, command.getKey(), true);
      } else {
         return invokeNext(ctx, command);
      }
   }

   protected Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ctx.removeLookedUpEntries(command.getAffectedKeys());
      }
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD && canRetrieveRemoteValue(command)) {
         return prefetchKeysIfNeededAndInvokeNext(ctx, command, command.getAffectedKeys(), true);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleReadManyCommand(ctx, command, command.getKeys());
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
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      // Need the full entry to filter out tombstones
      EntrySetCommand<K, V> entrySetCommand = commandsFactory.buildEntrySetCommand(command.getFlagsBitSet());
      return invokeNextThenApply(ctx, entrySetCommand, (rCtx, rCommand, rv) -> {
         boolean ignoreOwnership = rCommand.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL);
         return new BackingKeySet(ignoreOwnership, (CacheSet<CacheEntry<K, V>>) rv);
      });
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         boolean ignoreOwnership = rCommand.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL);
         return new BackingEntrySet(ignoreOwnership, (CacheSet<CacheEntry<K, V>>) rv);
      });
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      if (command.isGroupOwner()) {
         int segment = keyPartitioner.getSegment(command.getGroupName());
         switch (svm.getSegmentState(segment)) {
            case NOT_OWNED:
               // if we're not a primary owner of this segment, we don't have to wait for it
               break;
            case BLOCKED:
            case KEY_TRANSFER:
            case VALUE_TRANSFER:
               return asyncInvokeNext(ctx, command, svm.valuesFuture(command.getTopologyId()));
            case OWNED:
               break;
            default:
               throw new IllegalStateException();
         }
      }
      return invokeNext(ctx, command);
   }

   private Publisher<CacheEntry<K, V>> getPublisher(int segment, boolean ignoreOwnership,
                                                    CacheSet<CacheEntry<K, V>> next) {
      if (ignoreOwnership) {
         return next.localPublisher(segment);
      }
      ScatteredVersionManager.SegmentState segmentState = svm.getSegmentState(segment);
      switch (segmentState) {
         case NOT_OWNED:
            return Flowable.empty();
         case OWNED:
            return next.localPublisher(segment);
         case BLOCKED:
         case KEY_TRANSFER:
         case VALUE_TRANSFER:
            CompletionStage<Publisher<CacheEntry<K, V>>> stage =
                  svm.valuesFuture(dm.getCacheTopology().getTopologyId())
                     .thenApply(__ -> next.localPublisher(segment));
            return Single.fromCompletionStage(stage)
                         .flatMapPublisher(p -> p);
         default:
            throw new IllegalStateException();
      }
   }

   private class BackingEntrySet extends InternalCacheSet<CacheEntry<K, V>> {
      protected final CacheSet<CacheEntry<K, V>> next;
      private final boolean ignoreOwnership;

      public BackingEntrySet(boolean ignoreOwnership, CacheSet<CacheEntry<K, V>> next) {
         this.next = next;
         this.ignoreOwnership = ignoreOwnership;
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(IntSet segments) {
         // Wait for each segment independently
         return Flowable.fromIterable(segments)
                        .concatMap(this::localPublisher);
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(int segment) {
         return Flowable.fromPublisher(getPublisher(segment, ignoreOwnership, next))
                        .filter(e -> e.getValue() != null);
      }
   }

   private class BackingKeySet extends InternalCacheSet<K> {
      private final boolean ignoreOwnership;
      protected final CacheSet<CacheEntry<K, V>> next;

      public BackingKeySet(boolean ignoreOwnership, CacheSet<CacheEntry<K, V>> next) {
         this.ignoreOwnership = ignoreOwnership;
         this.next = next;
      }

      @Override
      public Publisher<K> localPublisher(IntSet segments) {
         // Wait for each segment independently
         return Flowable.fromIterable(segments)
                        .concatMap(this::localPublisher);
      }

      @Override
      public Publisher<K> localPublisher(int segment) {
         return Flowable.fromPublisher(getPublisher(segment, ignoreOwnership, next))
                        .mapOptional(e -> e.getValue() != null ? Optional.of(e.getKey()) : Optional.empty());
      }
   }
}
