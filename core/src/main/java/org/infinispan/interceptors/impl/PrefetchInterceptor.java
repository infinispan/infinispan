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
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
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
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
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
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
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
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class PrefetchInterceptor extends DDAsyncInterceptor {
   protected static Log log = LogFactory.getLog(PrefetchInterceptor.class);

   protected static final long STATE_TRANSFER_FLAGS = FlagBitSets.PUT_FOR_STATE_TRANSFER |
         FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP |
         FlagBitSets.SKIP_SHARED_CACHE_STORE | SKIP_OWNERSHIP_CHECK | FlagBitSets.SKIP_XSITE_BACKUP;
   protected ScatteredVersionManager svm;
   protected StateTransferManager stm;
   protected DistributionManager dm;
   protected KeyPartitioner keyPartitioner;
   protected CommandsFactory commandsFactory;
   protected RpcManager rpcManager;
   protected RpcOptions syncRpcOptions;
   protected Cache cache;
   protected int numSegments;
   // these are the same as in StateConsumerImpl

   @Inject
   public void injectDependencies(ScatteredVersionManager svm, StateTransferManager stm,
                                  DistributionManager dm, KeyPartitioner keyPartitioner, CommandsFactory commandsFactory,
                                  RpcManager rpcManager, Cache cache) {
      this.svm = svm;
      this.stm = stm;
      this.dm = dm;
      this.keyPartitioner = keyPartitioner;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.cache = cache;
   }

   @Start
   public void start() {
      this.syncRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      this.numSegments = cacheConfiguration.clustering().hash().numSegments();
   }

   private boolean canRetrieveRemoteValue(FlagAffectedCommand command) {
//      return (command.getFlagsBitSet() & LOCAL_FLAGS) == 0;
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

   private Object prefetchKeyIfNeededAndInvokeNext(InvocationContext ctx, VisitableCommand command, Object key, boolean isWrite) {
      int segment = keyPartitioner.getSegment(key);
      switch (svm.getSegmentState(segment)) {
         case NOT_OWNED:
            // if we're not a primary owner of this segment, we don't have to wait for it
            break;
         case BLOCKED:
            if (isWrite) {
               return asyncValue(svm.getBlockingFuture(segment).thenCompose(
                     nil -> makeStage(prefetchKeyIfNeededAndInvokeNext(ctx, command, key, true)).toCompletableFuture()));
            }
         case KEY_TRANSFER:
         case VALUE_TRANSFER:
            return asyncInvokeNext(ctx, command, retrieveSingleRemoteValue(ctx, key).toCompletableFuture());
         case OWNED:
            break;
         default:
            throw new IllegalStateException();
      }
      return invokeNext(ctx, command);
   }

   private Object prefetchKeysIfNeededAndInvokeNext(InvocationContext ctx, VisitableCommand command, Collection<?> keys, boolean isWrite) {
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
         return asyncInvokeNext(ctx, command, retrieveRemoteValues(ctx, transferedKeys).toCompletableFuture());
      } else {
         return invokeNext(ctx, command);
      }
   }

   private InvocationStage retrieveSingleRemoteValue(InvocationContext ctx, Object key) {
      GetCacheEntryCommand getCacheEntryCommand = commandsFactory.buildGetCacheEntryCommand(key, EnumUtil.bitSetOf(Flag.CACHE_MODE_LOCAL));
      return makeStage(invokeNextThenApply(ctx, getCacheEntryCommand, (ctx1, command1, rv) -> {
         CacheEntry entry = (CacheEntry) rv;
         Metadata metadata = entry != null ? entry.getMetadata() : null;
         CompletableFuture<InternalCacheValue> future;
         if (metadata != null && metadata.version() != null && svm.isVersionActual(keyPartitioner.getSegment(key), metadata.version())){
            return null;
         } else if ((metadata instanceof RemoteMetadata)) {
            Address backup = ((RemoteMetadata) metadata).getAddress();
            future = retrieveRemoteValue(Collections.singleton(backup), key);
         } else {
            future = retrieveRemoteValue(null, key);
         }
         return asyncValue(future.thenAccept(maxValue -> {
            if (maxValue == null) {
               return;
            }
            PutKeyValueCommand putKeyValueCommand = commandsFactory.buildPutKeyValueCommand(key, maxValue.getValue(), maxValue.getMetadata(), STATE_TRANSFER_FLAGS);
            invokeNext(ctx, putKeyValueCommand);
         }));
      }));
   }

   private CompletableFuture<InternalCacheValue> retrieveRemoteValue(Collection<Address> targets, Object key) {
      ClusteredGetCommand command = commandsFactory.buildClusteredGetCommand(key, FlagBitSets.SKIP_OWNERSHIP_CHECK);
      return rpcManager.invokeRemotelyAsync(targets, command, syncRpcOptions).thenApply(responseMap -> {
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
               // not sure if this can happen, but let's be on the safe side
               throw OutdatedTopologyException.INSTANCE;
            }
            if (metadata != null && metadata.version() != null) {
               if (maxVersion == null || maxVersion.compareTo(metadata.version()) == InequalVersionComparisonResult.BEFORE) {
                  maxVersion = metadata.version();
                  maxValue = icv;
               }
            }
         }
         return maxValue;
      });
   }

   private InvocationStage retrieveRemoteValues(InvocationContext ctx, List<?> keys) {
      ClusteredGetAllCommand command = commandsFactory.buildClusteredGetAllCommand(keys, FlagBitSets.SKIP_OWNERSHIP_CHECK, null);
      return asyncValue(rpcManager.invokeRemotelyAsync(null, command, syncRpcOptions).thenCompose(responseMap -> {
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
         if (map.isEmpty()) {
            return CompletableFutures.completedNull();
         }
         PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
         return makeStage(invokeNext(ctx, putMapCommand)).toCompletableFuture();
      }));
   }

   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) throws Throwable {
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
         return new BackingKeySet(getCacheWithFlags(command), ignoreOwnership, (Set<Object>) rv);
      });
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         boolean ignoreOwnership = command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
         return new BackingEntrySet(getCacheWithFlags(command), ignoreOwnership, (Set<CacheEntry>) rv);
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

   private class BackingEntrySet<K, V> extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V> implements CacheSet<CacheEntry<K, V>> {
      private final Set<CacheEntry<K, V>> entrySet;
      private final boolean ignoreOwnership;

      public BackingEntrySet(Cache<K, V> cache, boolean ignoreOwnership, Set<CacheEntry<K, V>> entrySet) {
         super(cache);
         this.ignoreOwnership = ignoreOwnership;
         this.entrySet = entrySet;
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         // Here we use stream because plain .iterator() would return non-serializable EntryWrapper entries
         return new BackingIterator<>(cache, ignoreOwnership, () -> entrySet.stream().iterator(), entry -> entry.getKey());
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         return  Closeables.spliterator(iterator(), Long.MAX_VALUE,
            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }

      @Override
      public boolean contains(Object o) {
         if (o instanceof Map.Entry) {
            V v = cache.get(((Map.Entry) o).getKey());
            return Objects.equals(v, ((Map.Entry) o).getValue());
         } else {
            return false;
         }
      }

      @Override
      public boolean remove(Object o) {
         if (o instanceof Map.Entry) {
            return cache.remove(((Map.Entry) o).getKey(), ((Map.Entry) o).getValue());
         } else {
            return false;
         }
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, dm.getReadConsistentHash(),
            () -> super.stream()), false, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, dm.getReadConsistentHash(),
            () -> super.stream()), true, cache.getAdvancedCache().getComponentRegistry());
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
            lastTopology = stm.getCacheTopology().getTopologyId();
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
         } while (stm.getCacheTopology().getTopologyId() != lastTopology);
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
               if (lastTopology == stm.getCacheTopology().getTopologyId()) {
                  int numSegments = cache.getCacheConfiguration().clustering().hash().numSegments();
                  boolean[] newFinishedSegments = finishedSegments == null ? new boolean[numSegments] : Arrays.copyOf(finishedSegments, numSegments);
                  for (int segment = 0; segment < numSegments; ++segment) {
                     if (svm.getSegmentState(segment) == ScatteredVersionManager.SegmentState.OWNED) {
                        newFinishedSegments[segment] = true;
                     }
                  }
                  // do one more check to find if the topology hasn't changed during iteration through states
                  if (lastTopology == stm.getCacheTopology().getTopologyId()) {
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

   private class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V> implements CacheSet<K> {
      private final Set<K> keySet;
      private final boolean ignoreOwnership;

      public BackingKeySet(Cache<K, V> cache, boolean ignoreOwnership, Set<K> keySet) {
         super(cache);
         this.ignoreOwnership = ignoreOwnership;
         this.keySet = keySet;
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new CloseableIterator<K>() {
            BackingIterator<K, K, V> iterator = new BackingIterator<>(cache, ignoreOwnership, () -> keySet.iterator(), Function.identity());

            @Override
            public void close() {
               iterator.close();
            }

            @Override
            public boolean hasNext() {
               return iterator.hasNext();
            }

            @Override
            public K next() {
               return iterator.next();
            }
         };
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return  Closeables.spliterator(iterator(), Long.MAX_VALUE,
            Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }

      @Override
      public boolean contains(Object o) {
         return cache.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      @Override
      public CacheStream<K> stream() {
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm.getReadConsistentHash(),
            () -> StreamSupport.stream(spliterator(), false)), false,
            cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<K> parallelStream() {
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm.getReadConsistentHash(),
            () -> StreamSupport.stream(spliterator(), false)), true,
            cache.getAdvancedCache().getComponentRegistry());
      }
   }
}
