package org.infinispan.interceptors.impl;

import static org.infinispan.context.impl.FlagBitSets.SKIP_OWNERSHIP_CHECK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
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
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingEntryCacheSet;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingKeyCacheSet;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class PrefetchInterceptor<K, V> extends DDAsyncInterceptor {
   protected static final Log log = LogFactory.getLog(PrefetchInterceptor.class);
   protected static final boolean trace = log.isTraceEnabled();

   protected static final long STATE_TRANSFER_FLAGS = FlagBitSets.PUT_FOR_STATE_TRANSFER |
         FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP |
         FlagBitSets.SKIP_SHARED_CACHE_STORE | SKIP_OWNERSHIP_CHECK | FlagBitSets.SKIP_XSITE_BACKUP;
   @Inject protected ScatteredVersionManager svm;
   @Inject protected DistributionManager dm;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected RpcManager rpcManager;
   @Inject protected Cache<K, V> cache;
   @Inject protected EntryFactory entryFactory;
   @Inject protected InternalDataContainer dataContainer;

   protected int numSegments;

   private final InvocationSuccessFunction handleRemotelyPrefetchedEntry = this::handleRemotelyPrefetchedEntry;

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
         return asyncValue(blockingFuture.thenCompose(
               nil -> makeStage(prefetchKeysIfNeededAndInvokeNext(ctx, command, keys, true)).toCompletableFuture()));
      }
      if (transferedKeys != null) {
         return asyncInvokeNext(ctx, command, retrieveRemoteValues(ctx, command, transferedKeys));
      } else {
         return invokeNext(ctx, command);
      }
   }

   private InvocationStage lookupLocalAndRetrieveRemote(InvocationContext ctx, Object key, DataCommand cmd, int segment) {
      // We need to lookup the dataContainer directly as GetCacheEntryCommand won't return entry with null value
      InternalCacheEntry entry = dataContainer.get(segment, key);
      if (trace) {
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
      if (trace) {
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
      return makeStage(asyncValue(remoteInvocation).thenApply(ctx, dataCommand, handleRemotelyPrefetchedEntry));
   }

   private Object handleRemotelyPrefetchedEntry(InvocationContext ctx, VisitableCommand command, Object rv) {
      Map<Address, Response> responseMap = (Map<Address, Response>) rv;
      EntryVersion maxVersion = null;
      InternalCacheValue maxValue = null;
      for (Response response : responseMap.values()) {
         if (!response.isSuccessful()) {
            throw OutdatedTopologyException.INSTANCE;
         }
         SuccessfulResponse successfulResponse = (SuccessfulResponse) response;
         InternalCacheValue icv = (InternalCacheValue) successfulResponse.getResponseValue();
         if (icv == null) {
            continue;
         }
         Metadata metadata = icv.getMetadata();
         if (metadata instanceof RemoteMetadata) {
            // Clustered get is sent with SKIP_OWNERSHIP_CHECK and that means that the topology won't be checked.
            // PrefetchInterceptor on the remote node won't try to fetch the value either, so retrieving remote value
            // from another node is possible.
            throw OutdatedTopologyException.INSTANCE;
         }
         if (metadata != null && metadata.version() != null) {
            if (maxVersion == null || maxVersion.compareTo(metadata.version()) == InequalVersionComparisonResult.BEFORE) {
               maxVersion = metadata.version();
               maxValue = icv;
            }
         }
      }
      if (trace) {
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
            dataCommand.getKey(), maxValue.getValue(), dataCommand.getSegment(), maxValue.getMetadata(), STATE_TRANSFER_FLAGS);
      putKeyValueCommand.setTopologyId(dataCommand.getTopologyId());
      return invokeNext(ctx, putKeyValueCommand);
   }

   // TODO: this is not completely aligned with single-entry prefetch
   private <C extends TopologyAffectedCommand & VisitableCommand> InvocationStage retrieveRemoteValues(InvocationContext ctx, C originCommand, List<?> keys) {
      if (trace) {
         log.tracef("Prefetching entries for keys %s using broadcast", keys);
      }
      ClusteredGetAllCommand command = commandsFactory.buildClusteredGetAllCommand(keys, FlagBitSets.SKIP_OWNERSHIP_CHECK, null);
      command.setTopologyId(originCommand.getTopologyId());
      CompletionStage<Map<Address, Response>> rpcFuture = rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
      return makeStage(asyncValue(rpcFuture).thenApply(ctx, originCommand, (rCtx, rCommand, rv) -> {
         TopologyAffectedCommand topologyAffectedCommand = (TopologyAffectedCommand) rCommand;
         Map<Address, Response> responseMap = (Map<Address, Response>) rv;
         InternalCacheValue[] maxValues = new InternalCacheValue[keys.size()];
         for (Response response : responseMap.values()) {
            if (!response.isSuccessful()) {
               throw OutdatedTopologyException.INSTANCE;
            }
            InternalCacheValue[] values = (InternalCacheValue[]) ((SuccessfulResponse) response).getResponseValue();
            int i = 0;
            for (InternalCacheValue icv : values) {
               if (icv != null) {
                  Metadata metadata = icv.getMetadata();
                  if (metadata instanceof RemoteMetadata) {
                     // not sure if this can happen, but let's be on the safe side
                     throw OutdatedTopologyException.INSTANCE;
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
         Map<Object, InternalCacheValue> map = new HashMap<>(keys.size());
         for (int i = 0; i < maxValues.length; ++i) {
            if (maxValues[i] != null) {
               map.put(keys.get(i), maxValues[i]);
            }
         }
         if (trace) {
            log.tracef("Prefetched values are %s", map);
         }
         if (map.isEmpty()) {
            return CompletableFutures.completedNull();
         }
         // The put below could fail updating the context if the data container got the updated value while we were
         // prefetching that. Also, we need to call RepeatableReadEntry.updatePreviousValue() to get return value
         // from the main command correct.
         for (Map.Entry<Object, InternalCacheValue> entry : map.entrySet()) {
            entryFactory.wrapExternalEntry(rCtx, entry.getKey(), entry.getValue().toInternalCacheEntry(entry.getKey()), true, true);
         }
         PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
         putMapCommand.setTopologyId(topologyAffectedCommand.getTopologyId());
         return invokeNext(rCtx, putMapCommand);
      }));
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

   protected Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
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

   public AdvancedCache getCacheWithFlags(FlagAffectedCommand command) {
      Set<Flag> flags = command.getFlags();
      return cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         boolean ignoreOwnership = command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
         return new BackingKeySet(ignoreOwnership, (CacheSet<K>) rv);
      });
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         boolean ignoreOwnership = command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
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

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   private class BackingEntrySet extends AbstractDelegatingEntryCacheSet<K, V> implements CacheSet<CacheEntry<K, V>> {
      private final CacheSet<CacheEntry<K, V>> entrySet;
      private final boolean ignoreOwnership;

      public BackingEntrySet(boolean ignoreOwnership, CacheSet<CacheEntry<K, V>> entrySet) {
         super(cache, entrySet);
         this.ignoreOwnership = ignoreOwnership;
         this.entrySet = entrySet;
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         // Here we use stream because plain .iterator() would return non-serializable EntryWrapper entries
         return new BackingIterator<>(cache, ignoreOwnership, () -> entrySet.stream().iterator(), Map.Entry::getKey);
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         return  Closeables.spliterator(iterator(), Long.MAX_VALUE,
            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }
   }

   private class BackingIterator<O, K, V> implements CloseableIterator<O> {
      private final Cache<K, V> cache;
      private final Supplier<Iterator<O>> supplier;
      private final Function<O, K> keyRetrieval;
      private final boolean ignoreOwnership;
      private Iterator<O> iterator;
      private K previousKey;
      private O next;
      private List<Integer> blockedSegments;
      private int lastTopology;
      private boolean[] finishedSegments;

      @Override
      public void remove() {
         if (previousKey == null) {
            throw new IllegalStateException();
         }
         cache.remove(previousKey);
         previousKey = null;
      }

      public BackingIterator(Cache<K, V> cache, boolean ignoreOwnership, Supplier<Iterator<O>> supplier, Function<O, K> keyRetrieval) {
         this.cache = cache;
         this.ignoreOwnership = ignoreOwnership;
         this.supplier = supplier;
         log.tracef("Retrieving iterator for %s for the first time", cache);
         this.iterator = supplier.get();
         this.keyRetrieval = keyRetrieval;
         findNotReadySegments();
      }

      protected void findNotReadySegments() {
         if (ignoreOwnership) {
            return;
         }
         do {
            lastTopology = dm.getCacheTopology().getTopologyId();
            int numSegments = cache.getCacheConfiguration().clustering().hash().numSegments();
            if (blockedSegments != null) {
               blockedSegments.clear();
            }
            for (int segment = 0; segment < numSegments; ++segment) {
               switch (svm.getSegmentState(segment)) {
                  case NOT_OWNED:
                     break;
                  case BLOCKED:
                  case KEY_TRANSFER:
                  case VALUE_TRANSFER:
                     addBlocked(segment);
                     break;
                  case OWNED:
                     break;
               }
            }
         } while (dm.getCacheTopology().getTopologyId() != lastTopology);
      }

      private void addBlocked(int segment) {
         if (blockedSegments == null) {
            blockedSegments = new ArrayList<>();
         }
         blockedSegments.add(segment);
      }

      @Override
      public boolean hasNext() {
         if (iterator == null) {
            next = null;
            return false;
         }
         for (;;) {
            while (iterator.hasNext()) {
               next = iterator.next();
               if (ignoreOwnership) {
                  return true;
               }
               int segment = keyPartitioner.getSegment(keyRetrieval.apply(next));
               if (finishedSegments == null || !finishedSegments[segment]) {
                  if (svm.getSegmentState(segment) == ScatteredVersionManager.SegmentState.OWNED) {
                     return true;
                  }
               }
            }
            if (blockedSegments != null && !blockedSegments.isEmpty()) {
               if (lastTopology == dm.getCacheTopology().getTopologyId()) {
                  int numSegments = cache.getCacheConfiguration().clustering().hash().numSegments();
                  boolean[] newFinishedSegments = finishedSegments == null ? new boolean[numSegments] : Arrays.copyOf(finishedSegments, numSegments);
                  for (int segment = 0; segment < numSegments; ++segment) {
                     if (svm.getSegmentState(segment) == ScatteredVersionManager.SegmentState.OWNED) {
                        newFinishedSegments[segment] = true;
                     }
                  }
                  // do one more check to find if the topology hasn't changed during iteration through states
                  if (lastTopology == dm.getCacheTopology().getTopologyId()) {
                     finishedSegments = newFinishedSegments;
                  }
               }
               try {
                  svm.valuesFuture(lastTopology).get(cacheConfiguration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
               } catch (Exception e) {
                  throw new CacheException(e);
               }
               findNotReadySegments();
               if (iterator instanceof CloseableIterator) {
                  ((CloseableIterator) iterator).close();
               }
               log.tracef("Retrieving iterator for %s in topology %d, blocked segments are %s", cache, lastTopology, blockedSegments);
               iterator = supplier.get();
            } else {
               return false;
            }
         }
      }

      @Override
      public O next() {
         if (next == null && !hasNext()) {
            throw new NoSuchElementException();
         }
         assert next != null;
         previousKey = keyRetrieval.apply(next);
         return next;
      }

      @Override
      public void close() {
         if (iterator instanceof CloseableIterator) {
            ((CloseableIterator) iterator).close();
         }
         iterator = null;
      }
   }

   private class BackingKeySet extends AbstractDelegatingKeyCacheSet<K, V> implements CacheSet<K> {
      private final boolean ignoreOwnership;

      public BackingKeySet(boolean ignoreOwnership, CacheSet<K> keySet) {
         super(cache, keySet);
         this.ignoreOwnership = ignoreOwnership;
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new BackingIterator<>(cache, ignoreOwnership, delegate()::iterator, Function.identity());
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return  Closeables.spliterator(iterator(), Long.MAX_VALUE,
            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }
   }
}
