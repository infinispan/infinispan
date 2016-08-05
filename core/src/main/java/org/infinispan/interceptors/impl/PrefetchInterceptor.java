package org.infinispan.interceptors.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.functional.*;
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
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
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
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static org.infinispan.context.Flag.*;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class PrefetchInterceptor extends DDAsyncInterceptor {
   protected static Log log = LogFactory.getLog(PrefetchInterceptor.class);

   protected static final long STATE_TRANSFER_FLAGS = EnumUtil.bitSetOf(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL,
      IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
      SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK,
      SKIP_XSITE_BACKUP);
   private static final AsyncInterceptor.ForkReturnHandler DUMMY_FORK_RETURN_HANDLER = (ctx2, cmd, rv, t) -> {
      if (t != null) throw t;
      return ctx2.continueInvocation();
   };
   protected ScatteredVersionManager svm;
   protected StateTransferManager stm;
   protected DistributionManager dm;
   protected CommandsFactory commandsFactory;
   protected RpcManager rpcManager;
   protected RpcOptions syncRpcOptions;
   protected Cache cache;
   // these are the same as in StateConsumerImpl

   @Inject
   public void injectDependencies(ScatteredVersionManager svm, StateTransferManager stm,
                                  DistributionManager dm, CommandsFactory commandsFactory,
                                  RpcManager rpcManager, Cache cache) {
      this.svm = svm;
      this.stm = stm;
      this.dm = dm;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.cache = cache;
   }

   @Start
   public void start() {
      this.syncRpcOptions = rpcManager.getDefaultRpcOptions(true);
   }

   private boolean canRetrieveRemoteValue(LocalFlagAffectedCommand command) {
//      return (command.getFlagsBitSet() & LOCAL_FLAGS) == 0;
      // We have to do prefetch for remote-originating reads as these have local flag set
      return !command.hasFlag(SKIP_OWNERSHIP_CHECK);
   }

   protected CompletableFuture<Void> handleReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      if (command.hasFlag(COMMAND_RETRY)) {
         ctx.removeLookedUpEntry(command.getKey());
      }
      if (canRetrieveRemoteValue(command)) {
         return prefetchKeyIfNeeded(ctx, command.getKey());
      } else {
         return ctx.continueInvocation();
      }
   }

   private CompletableFuture<Void> prefetchKeyIfNeeded(InvocationContext ctx, Object key) throws Throwable {
      int segment = dm.getWriteConsistentHash().getSegment(key);
      switch (svm.getSegmentState(segment)) {
         case NOT_OWNED:
            // if we're not a primary owner of this segment, we don't have to wait for it
            break;
         case BLOCKED:
         case KEY_TRANSFER:
//            return retrieveRemoteValue(ctx, null, key);
         case VALUE_TRANSFER:
            return retrieveRemoteValueFromSingleNode(ctx, key);
         case OWNED:
            break;
         default:
            throw new IllegalStateException();
      }
      return ctx.continueInvocation();
   }

   private CompletableFuture<Void> prefetchKeysIfNeeded(InvocationContext ctx, Collection<?> keys) throws Throwable {
      List<Object> transferedKeys = null;
      for (Object key : keys) {
         int segment = dm.getWriteConsistentHash().getSegment(key);
         switch (svm.getSegmentState(segment)) {
            case NOT_OWNED:
               // if we're not a primary owner of this segment, we don't have to wait for it
               break;
            case BLOCKED:
            case KEY_TRANSFER:
            case VALUE_TRANSFER:
               // TODO: single node logic
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
      if (transferedKeys != null) {
         return retrieveRemoteValues(ctx, transferedKeys);
      } else {
         return ctx.continueInvocation();
      }
   }

   private CompletableFuture<Void> retrieveRemoteValueFromSingleNode(InvocationContext ctx, Object key) {
      // Find out who is the backup
      // TODO: without cache stores, load it from DC directly
      GetCacheEntryCommand getCacheEntryCommand = commandsFactory.buildGetCacheEntryCommand(key, EnumUtil.bitSetOf(Flag.CACHE_MODE_LOCAL));
      return ctx.forkInvocation(getCacheEntryCommand, (ctx1, command1, rv, throwable) -> {
         if (throwable != null) {
            throw throwable;
         }
         CacheEntry entry = (CacheEntry) rv;
         Metadata metadata = entry != null ? entry.getMetadata() : null;
         if (metadata != null && metadata.version() != null && svm.isVersionActual(dm.getConsistentHash().getSegment(key), ((NumericVersion) metadata.version()).getVersion())){
            return ctx.continueInvocation();
         } else if ((metadata instanceof RemoteMetadata)) {
            Address backup = ((RemoteMetadata) metadata).getAddress();
            return retrieveRemoteValue(ctx1, Collections.singleton(backup), key);
         } else {
            return retrieveRemoteValue(ctx1, dm.getWriteConsistentHash().getMembers(), key);
         }
      });
   }

   private CompletableFuture<Void> retrieveRemoteValue(InvocationContext ctx, Collection<Address> targets, Object key) {
      ClusteredGetCommand command = commandsFactory.buildClusteredGetCommand(key, 0, false, null);
      command.addFlag(Flag.SKIP_OWNERSHIP_CHECK);
      return rpcManager.invokeRemotelyAsync(targets, command, syncRpcOptions).thenCompose(responseMap -> {
         EntryVersion maxVersion = null;
         InternalCacheValue maxValue = null;
         for (Response response : responseMap.values()) {
            if (!response.isSuccessful()) {
               // TODO? Just try again...
               throw new OutdatedTopologyException("Some response was unsuccessful: " + responseMap);
            }
            SuccessfulResponse successfulResponse = (SuccessfulResponse) response;
            if (successfulResponse.getResponseValue() == null) {
               continue;
            }
            InternalCacheValue icv = (InternalCacheValue) successfulResponse.getResponseValue();
            Metadata metadata = icv.getMetadata();
            if (metadata instanceof RemoteMetadata) {
               // not sure if this can happen, but let's be safe
               throw new OutdatedTopologyException("The other node had state-transfer-temporary value: " + responseMap);
            }
            if (metadata != null && metadata.version() != null) {
               if (maxVersion == null || maxVersion.compareTo(metadata.version()) == InequalVersionComparisonResult.BEFORE) {
                  maxVersion = metadata.version();
                  maxValue = icv;
               }
            }
         }
         if (maxValue != null) {
            PutKeyValueCommand putKeyValueCommand = commandsFactory.buildPutKeyValueCommand(key, maxValue.getValue(), maxValue.getMetadata(), STATE_TRANSFER_FLAGS);
            return ctx.forkInvocation(putKeyValueCommand, DUMMY_FORK_RETURN_HANDLER);
         } else {
            try {
               return ctx.continueInvocation();
            } catch (Throwable throwable) {
               throw new CacheException(throwable);
            }
         }
      });
   }

   private CompletableFuture<Void> retrieveRemoteValues(InvocationContext ctx, List<?> keys) {
      ClusteredGetAllCommand command = commandsFactory.buildClusteredGetAllCommand(keys, 0, null);
      command.addFlag(Flag.SKIP_OWNERSHIP_CHECK);
      // we don't use target == null because one of the nodes the cache could be stopped, we would get SuspectException
      // and STI would wait for increased topology
      return rpcManager.invokeRemotelyAsync(dm.getWriteConsistentHash().getMembers(), command, syncRpcOptions).thenCompose(responseMap -> {
         InternalCacheValue[] maxValues = new InternalCacheValue[keys.size()];
         for (Response response : responseMap.values()) {
            if (!response.isSuccessful()) {
               // TODO? Just try again...
               throw new OutdatedTopologyException("Some response was unsuccessful: " + responseMap);
            }
            List<InternalCacheValue> values = (List<InternalCacheValue>) ((SuccessfulResponse) response).getResponseValue();
            int i = 0;
            for (InternalCacheValue icv : values) {
               if (icv != null) {
                  Metadata metadata = icv.getMetadata();
                  if (metadata instanceof RemoteMetadata) {
                     // not sure if this can happen, but let's be safe
                     throw new OutdatedTopologyException("The other node had state-transfer-temporary value: " + responseMap);
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
         HashMap map = new HashMap(keys.size());
         for (int i = 0; i < maxValues.length; ++i) {
            if (maxValues[i] != null) {
               map.put(keys.get(i), maxValues[i]);
            }
         }
         if (!map.isEmpty()) {
            PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
            return ctx.forkInvocation(putMapCommand, DUMMY_FORK_RETURN_HANDLER);
         } else {
            try {
               return ctx.continueInvocation();
            } catch (Throwable throwable) {
               throw new CacheException(throwable);
            }
         }
      });
   }

   protected CompletableFuture<Void> handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) throws Throwable {
      if (command.hasFlag(COMMAND_RETRY)) {
         for (Object key : keys) {
            ctx.removeLookedUpEntry(key);
         }
      }
      if (canRetrieveRemoteValue(command)) {
         return prefetchKeysIfNeeded(ctx, keys);
      } else {
         return ctx.continueInvocation();
      }
   }

   protected CompletableFuture<Void> handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (command.hasFlag(COMMAND_RETRY)) {
         ctx.removeLookedUpEntry(command.getKey());
      }
      if (command.readsExistingValues() && canRetrieveRemoteValue(command)) {
         return prefetchKeyIfNeeded(ctx, command.getKey());
      } else {
         return ctx.continueInvocation();
      }
   }

   protected CompletableFuture<Void> handleWriteManyCommand(InvocationContext ctx, FlagAffectedCommand command, Set<Object> keys) throws Throwable {
      if (command.hasFlag(COMMAND_RETRY)) {
         for (Object key : keys) {
            ctx.removeLookedUpEntry(key);
         }
      }
      if (command.readsExistingValues() && canRetrieveRemoteValue(command)) {
         return prefetchKeysIfNeeded(ctx, keys);
      } else {
         return ctx.continueInvocation();
      }
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleReadManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return super.visitReadOnlyKeyCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return super.visitReadOnlyManyCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getMap().keySet());
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, command.getKeys());
   }

   public AdvancedCache getCacheWithFlags(LocalFlagAffectedCommand command) {
      Set<Flag> flags = command.getFlags();
      return cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
   }

   @Override
   public CompletableFuture<Void> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            throw throwable;
         } else {
            boolean ignoreOwnership = command.hasFlag(SKIP_OWNERSHIP_CHECK);
            return CompletableFuture.completedFuture(new BackingKeySet(getCacheWithFlags(command), ignoreOwnership, (Set<Object>) rv));
         }
      });
   }

   @Override
   public CompletableFuture<Void> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            throw throwable;
         } else {
            boolean ignoreOwnership = command.hasFlag(SKIP_OWNERSHIP_CHECK);
            return CompletableFuture.completedFuture(new BackingEntrySet(getCacheWithFlags(command), ignoreOwnership, (Set<CacheEntry>) rv));
         }
      });
   }

   @Override
   public CompletableFuture<Void> visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      if (command.isGroupOwner()) {
         // TODO: this is a blocking implementation - we could ask all other nodes
         int segment = dm.getWriteConsistentHash().getSegment(command.getGroupName());
         switch (svm.getSegmentState(segment)) {
            case NOT_OWNED:
               // if we're not a primary owner of this segment, we don't have to wait for it
               break;
            case BLOCKED:
            case KEY_TRANSFER:
            case VALUE_TRANSFER:
               svm.waitForValues(command.getTopologyId());
               break;
            case OWNED:
               break;
            default:
               throw new IllegalStateException();
         }
      }
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
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
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, dm.getWriteConsistentHash(),
            () -> super.stream()), false, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(cache, dm.getWriteConsistentHash(),
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
               ConsistentHash ch = dm.getConsistentHash();
               int segment = ch.getSegment(keyRetrieval.apply(next));
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
               svm.waitForValues(lastTopology);
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
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm.getWriteConsistentHash(),
            () -> StreamSupport.stream(spliterator(), false)), false,
            cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public CacheStream<K> parallelStream() {
         return new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm.getWriteConsistentHash(),
            () -> StreamSupport.stream(spliterator(), false)), true,
            cache.getAdvancedCache().getComponentRegistry());
      }
   }
}
