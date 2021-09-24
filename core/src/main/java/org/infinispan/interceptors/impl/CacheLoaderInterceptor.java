package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.NOT_ASYNC;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.SHARED;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.Caches;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupFilter;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager.StoreChangeListener;
import org.infinispan.persistence.manager.PersistenceStatus;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.stream.impl.local.PersistenceEntryStreamSupplier;
import org.infinispan.stream.impl.local.PersistenceKeyStreamSupplier;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;
import org.infinispan.util.EntryWrapper;
import org.infinispan.util.LazyConcatIterator;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * @since 9.0
 */
@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor<K, V> extends JmxStatsCommandInterceptor implements EntryLoader<K, V>, StoreChangeListener {
   private static final Log log = LogFactory.getLog(CacheLoaderInterceptor.class);

   protected final AtomicLong cacheLoads = new AtomicLong(0);
   protected final AtomicLong cacheMisses = new AtomicLong(0);

   @Inject protected PersistenceManager persistenceManager;
   @Inject protected CacheNotifier notifier;
   @Inject protected EntryFactory entryFactory;
   @Inject TimeService timeService;
   @Inject InternalEntryFactory iceFactory;
   @Inject InternalDataContainer<K, V> dataContainer;
   @Inject GroupManager groupManager;
   @Inject ComponentRef<Cache<K, V>> cache;
   @Inject KeyPartitioner partitioner;
   @Inject @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   protected ExecutorService nonBlockingExecutor;

   protected boolean activation;
   private volatile boolean usingStores;

   private final ConcurrentMap<Object, CompletionStage<InternalCacheEntry<K, V>>> pendingLoads = new ConcurrentHashMap<>();

   @Start
   public void start() {
      this.activation = cacheConfiguration.persistence().passivation();
      this.usingStores = cacheConfiguration.persistence().usingStores();
   }

   @Override
   public void storeChanged(PersistenceStatus persistenceStatus) {
      usingStores = persistenceStatus.isEnabled();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx,
                                           GetCacheEntryCommand command) {
      return visitDataCommand(ctx, command);
   }


   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) {
      return visitManyDataCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return visitDataCommand(ctx, command);
   }

   private Object visitManyDataCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) {
      AggregateCompletionStage<Void> stage = null;
      for (Object key : keys) {
         CompletionStage<?> innerStage = loadIfNeeded(ctx, key, command);
         if (innerStage != null && !CompletionStages.isCompletedSuccessfully(innerStage)) {
            if (stage == null) {
               stage = CompletionStages.aggregateCompletionStage();
            }
            stage.dependsOn(innerStage);
         }
      }
      if (stage != null) {
         return asyncInvokeNext(ctx, command, stage.freeze());
      }
      return invokeNext(ctx, command);
   }

   private Object visitDataCommand(InvocationContext ctx, AbstractDataCommand command) {
      Object key;
      CompletionStage<?> stage = null;
      if ((key = command.getKey()) != null) {
         stage = loadIfNeeded(ctx, key, command);
      }
      return asyncInvokeNext(ctx, command, stage);
   }

   @Override
   public Object visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command) {
      if (!command.isGroupOwner() || hasSkipLoadFlag(command)) {
         return invokeNext(ctx, command);
      }

      final Predicate<? super K> keyFilter = new GroupFilter<>(command.getGroupName(), groupManager)
                                                .and(k -> ctx.lookupEntry(k) == null);

      Publisher<MarshallableEntry<K, V>> publisher = persistenceManager.publishEntries(keyFilter, true, false,
            PersistenceManager.AccessMode.BOTH);
      CompletionStage<InternalCacheEntry<K, V>> publisherStage = Flowable.fromPublisher(publisher)
            .map(me -> PersistenceUtil.convert(me, iceFactory))
            .doOnNext(ice -> entryFactory.wrapExternalEntry(ctx, ice.getKey(), ice, true, false))
            .lastElement()
            .toCompletionStage(null);
      return asyncInvokeNext(ctx, command, publisherStage);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command)
         throws Throwable {
      // Acquire the remote iteration flag and set it for all below - so they won't wrap unnecessarily
      boolean isRemoteIteration = command.hasAnyFlag(FlagBitSets.REMOTE_ITERATION);
      command.addFlags(FlagBitSets.REMOTE_ITERATION);
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (hasSkipLoadFlag(command)) {
            // Continue with the existing throwable/return value
            return rv;
         }
         CacheSet<CacheEntry<K, V>> entrySet = (CacheSet<CacheEntry<K, V>>) rv;
         return new WrappedEntrySet(command, isRemoteIteration, entrySet);
      });
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command)
         throws Throwable {
      // Acquire the remote iteration flag and set it for all below - so they won't wrap unnecessarily
      boolean isRemoteIteration = command.hasAnyFlag(FlagBitSets.REMOTE_ITERATION);
      command.addFlags(FlagBitSets.REMOTE_ITERATION);
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (hasSkipLoadFlag(command)) {
            // Continue with the existing throwable/return value
            return rv;
         }

         CacheSet<K> keySet = (CacheSet<K>) rv;
         return new WrappedKeySet(command, isRemoteIteration, keySet);
      });
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) {
      return visitManyDataCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return visitDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return visitManyDataCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return visitManyDataCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) {
      CompletionStage<Long> sizeStage = trySizeOptimization(command.getFlagsBitSet());
      return asyncValue(sizeStage).thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if ((Long) rv == -1) {
            return super.visitSizeCommand(rCtx, rCommand);
         }
         return rv;
      });

   }

   private CompletionStage<Long> trySizeOptimization(long flagBitSet) {
      if (EnumUtil.containsAny(flagBitSet, FlagBitSets.SKIP_CACHE_LOAD | FlagBitSets.SKIP_SIZE_OPTIMIZATION)) {
         return CompletableFuture.completedFuture(-1L);
      }
      // Get the size from any shared store that isn't async
      return persistenceManager.size(SHARED.and(NOT_ASYNC));
   }

   protected final boolean isConditional(WriteCommand cmd) {
      return cmd.isConditional();
   }

   protected final boolean hasSkipLoadFlag(FlagAffectedCommand cmd) {
      return cmd.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD);
   }

   protected boolean canLoad(Object key, int segment) {
      return true;
   }

   /**
    * Loads from the cache loader the entry for the given key.  A found value is loaded into the current context.  The
    * method returns whether the value was found or not, or even if the cache loader was checked.
    * @param ctx The current invocation's context
    * @param key The key for the entry to look up
    * @param cmd The command that was called that now wants to query the cache loader
    * @return null or a CompletionStage that when complete all listeners will be notified
    * @throws Throwable
    */
   protected final CompletionStage<?> loadIfNeeded(final InvocationContext ctx, Object key, final FlagAffectedCommand cmd) {
      int segment = SegmentSpecificCommand.extractSegment(cmd, key, partitioner);
      if (skipLoad(ctx, key, segment, cmd)) {
         return null;
      }

      return loadInContext(ctx, key, segment, cmd);
   }

   /**
    * Attemps to load the given entry for a key from the persistence store. This method optimizes concurrent loads
    * of the same key so only the first is actually loaded. The additional loads will in turn complete when the
    * first completes, which provides minimal hits to the backing store(s).
    * @param ctx context for this invocation
    * @param key key to find the entry for
    * @param segment the segment of the key
    * @param cmd the command that initiated this load
    * @return a stage that when complete will have the entry loaded into the provided context
    */
   protected CompletionStage<?> loadInContext(InvocationContext ctx, Object key, int segment, FlagAffectedCommand cmd) {
      CompletableFuture<InternalCacheEntry<K, V>> cf = new CompletableFuture<>();

      CompletionStage<InternalCacheEntry<K, V>> otherCF = pendingLoads.putIfAbsent(key, cf);
      if (otherCF != null) {
         // Nothing to clean up, just put the entry from the other load in the context
         if (log.isTraceEnabled()) {
            log.tracef("Piggybacking on concurrent load for key %s", key);
         }
         // Resume on a different CPU thread so we don't have to wait until the other command completes
         return otherCF.thenAcceptAsync(entry -> putInContext(ctx, key, cmd, entry), nonBlockingExecutor);
      }

      CompletionStage<InternalCacheEntry<K, V>> result = loadAndStoreInDataContainer(ctx, key, segment, cmd);
      if (CompletionStages.isCompletedSuccessfully(result)) {
         finishLoadInContext(ctx, key, cmd, cf, CompletionStages.join(result), null);
      } else {
         result.whenComplete((value, throwable) -> finishLoadInContext(ctx, key, cmd, cf, value, throwable));
      }
      return cf;
   }

   private void finishLoadInContext(InvocationContext ctx, Object key, FlagAffectedCommand cmd, CompletableFuture<InternalCacheEntry<K, V>> cf, InternalCacheEntry<K, V> value, Throwable throwable) {
      // Make sure we clean up our pendingLoads properly and before completing any responses
      pendingLoads.remove(key);
      if (throwable != null) {
         cf.completeExceptionally(throwable);
      } else {
         putInContext(ctx, key, cmd, value);

         cf.complete(value);
      }
   }

   private void putInContext(InvocationContext ctx, Object key, FlagAffectedCommand cmd, InternalCacheEntry<K, V> entry) {
      if (entry != null) {
         entryFactory.wrapExternalEntry(ctx, key, entry, true, cmd instanceof WriteCommand);
      }
      CacheEntry contextEntry = ctx.lookupEntry(key);
      if (contextEntry instanceof MVCCEntry) {
         ((MVCCEntry) contextEntry).setLoaded(true);
      }
   }

   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      InternalCacheEntry<K, V> entry = dataContainer.peek(segment, key);
      boolean includeStores = true;
      if (entry != null) {
         if (!entry.canExpire() || !entry.isExpired(timeService.wallClockTime())) {
            return CompletableFuture.completedFuture(entry);
         }
         includeStores = false;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Loading entry for key %s", key);
      }
      CompletionStage<InternalCacheEntry<K, V>> resultStage = persistenceManager.<K, V>loadFromAllStores(key, segment,
            ctx.isOriginLocal(), includeStores).thenApply(me -> {
         if (me != null) {
            InternalCacheEntry<K, V> ice = PersistenceUtil.convert(me, iceFactory);
            if (getStatisticsEnabled()) {
               cacheLoads.incrementAndGet();
            }
            if (log.isTraceEnabled()) {
               log.tracef("Loaded entry: %s for key %s from store and attempting to insert into data container",
                     ice, key);
            }

            DataContainer.ComputeAction<K, V> putIfAbsentOrExpired = (k, oldEntry, factory) -> {
               if (oldEntry != null &&
                     (!oldEntry.canExpire() || !oldEntry.isExpired(timeService.wallClockTime()))) {
                  return oldEntry;
               }
               if (ice.canExpire()) {
                  ice.touch(timeService.wallClockTime());
               }
               return ice;
            };

            dataContainer.compute(segment, (K) key, putIfAbsentOrExpired);
            return ice;
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Missed entry load for key %s from store", key);
            }
            if (getStatisticsEnabled()) {
               cacheMisses.incrementAndGet();
            }
            return null;
         }
      });

      if (notifier.hasListener(CacheEntryLoaded.class) || notifier.hasListener(CacheEntryActivated.class)) {
         return resultStage.thenCompose(ice -> {
            if (ice != null) {
               V value = ice.getValue();
               CompletionStage<Void> notificationStage = sendNotification(key, value, true, ctx, cmd);
               notificationStage = notificationStage.thenCompose(v -> sendNotification(key, value, false, ctx, cmd));
               return notificationStage.thenApply(ignore -> ice);
            } else {
               return CompletableFutures.completedNull();
            }
         });
      }
      return resultStage;
   }

   private boolean skipLoad(InvocationContext ctx, Object key, int segment, FlagAffectedCommand cmd) {
      CacheEntry e = ctx.lookupEntry(key);
      if (e == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s. Entry is not in the context.", cmd);
         }
         return true;
      }
      if (e.getValue() != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s. Entry %s (skipLookup=%s) has non-null value.", cmd, e, e.skipLookup());
         }
         return true;
      }
      if (e.skipLookup()) {
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s. Entry %s (skipLookup=%s) is set to skip lookup.", cmd, e, e.skipLookup());
         }
         return true;
      }

      if (!cmd.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK) && !canLoad(key, segment)) {
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s. Cannot load the key.", cmd);
         }
         return true;
      }

      boolean skip;
      if (cmd instanceof WriteCommand) {
         skip = skipLoadForWriteCommand((WriteCommand) cmd, key, ctx);
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for write command %s? %s", cmd, skip);
         }
      } else {
         //read command
         skip = hasSkipLoadFlag(cmd);
         if (log.isTraceEnabled()) {
            log.tracef("Skip load for command %s?. %s", cmd, skip);
         }
      }
      return skip;
   }

   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      // TODO loading should be mandatory if there are listeners for previous values
      if (cmd.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
         if (hasSkipLoadFlag(cmd)) {
            log.tracef("Skipping load for command that reads existing values %s", cmd);
            return true;
         } else {
            return false;
         }
      }
      return true;
   }

   protected CompletionStage<Void> sendNotification(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand cmd) {
      CompletionStage<Void> stage = notifier.notifyCacheEntryLoaded(key, value, pre, ctx, cmd);
      if (activation) {
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            stage = notifier.notifyCacheEntryActivated(key, value, pre, ctx, cmd);
         } else {
            stage = CompletionStages.allOf(stage, notifier.notifyCacheEntryActivated(key, value, pre, ctx, cmd));
         }
      }
      return stage;
   }

   @ManagedAttribute(
         description = "Number of entries loaded from cache store",
         displayName = "Number of cache store loads",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderLoads() {
      return cacheLoads.get();
   }

   @ManagedAttribute(
         description = "Number of entries that did not exist in cache store",
         displayName = "Number of cache store load misses",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderMisses() {
      return cacheMisses.get();
   }

   @Override
   public void resetStatistics() {
      cacheLoads.set(0);
      cacheMisses.set(0);
   }

   /**
    * This method returns a collection of cache loader types (fully qualified class names) that are configured and enabled.
    */
   @ManagedAttribute(
         description = "Returns a collection of cache loader types which are configured and enabled",
         displayName = "Returns a collection of cache loader types which are configured and enabled")
   public Collection<String> getStores() {
      if (usingStores) {
         return persistenceManager.getStoresAsString();
      } else {
         return Collections.emptySet();
      }
   }

   /**
    * Disables a store of a given type.
    *
    * If the given type cannot be found, this is a no-op.  If more than one store of the same type is configured,
    * all stores of the given type are disabled.
    *
    * @param storeType fully qualified class name of the cache loader type to disable
    */
   @ManagedOperation(
         description = "Disable all stores of a given type, where type is a fully qualified class name of the cache loader to disable",
         displayName = "Disable all stores of a given type"
   )
   public void disableStore(@Parameter(name = "storeType", description = "Fully qualified class name of a store implementation") String storeType) {
      persistenceManager.disableStore(storeType);
   }

   private abstract class AbstractLoaderSet<R> extends AbstractSet<R> implements CacheSet<R> {
      protected final CacheSet<R> cacheSet;
      protected final long commandFlagBitSet;

      AbstractLoaderSet(CacheSet<R> cacheSet, long commandFlagBitSet) {
         this.cacheSet = cacheSet;
         this.commandFlagBitSet = commandFlagBitSet;
      }

      protected abstract CloseableIterator<R> innerIterator();

      @Override
      public CloseableSpliterator<R> spliterator() {
         return new IteratorAsSpliterator.Builder<>(innerIterator())
               .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL).get();
      }

      @Override
      public void clear() {
         cache.wired().clear();
      }

      @Override
      public int size() {
         long size = CompletionStages.join(trySizeOptimization(commandFlagBitSet));
         if (size >= 0) {
            return (int) Math.min(size, Integer.MAX_VALUE);
         }

         long longSize = stream().count();
         if (longSize > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
         }
         return (int) longSize;
      }

      @Override
      public boolean isEmpty() {
         boolean empty = cacheSet.isEmpty();
         // Check the store if the data container was empty
         if (empty) {
            Single<Boolean> emptySingle =
                  Flowable.fromPublisher(persistenceManager.publishKeys(null, PersistenceManager.AccessMode.BOTH))
                  .isEmpty();
            @SuppressWarnings("checkstyle:forbiddenmethod")
            boolean emptyReturn = emptySingle.blockingGet();
            empty = emptyReturn;
         }
         return empty;
      }

      @Override
      public CacheStream<R> stream() {
         return getStream(false);
      }

      @Override
      public CacheStream<R> parallelStream() {
         return getStream(true);
      }

      abstract protected CacheStream<R> getStream(boolean parallel);

      protected abstract AbstractLocalCacheStream.StreamSupplier<R, Stream<R>> supplier();
   }

   private class WrappedEntrySet extends AbstractLoaderSet<CacheEntry<K, V>> {
      private final Cache<K, V> cache;
      private final boolean isRemoteIteration;

      public WrappedEntrySet(EntrySetCommand command, boolean isRemoteIteration, CacheSet<CacheEntry<K, V>> entrySet) {
         super(entrySet, command.getFlagsBitSet());
         this.cache = Caches.getCacheWithFlags(CacheLoaderInterceptor.this.cache.wired(), command);
         this.isRemoteIteration = isRemoteIteration;
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry entry = toEntry(o);
         // Remove must be done by the cache
         return entry != null && cache.remove(entry.getKey(), entry.getValue());
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         if (isRemoteIteration) {
            return innerIterator();
         }
         return new IteratorMapper<>(new RemovableCloseableIterator<>(innerIterator(),
               e -> cache.remove(e.getKey(), e.getValue())), e -> new EntryWrapper<>(cache, e));
      }

      protected CloseableIterator<CacheEntry<K, V>> innerIterator() {
         // This can be a HashSet since it is only written to from the local iterator which is only invoked
         // from user thread
         Set<K> seenKeys = new HashSet<>(cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
         CloseableIterator<CacheEntry<K, V>> localIterator = new IteratorMapper<>(cacheSet.iterator(), e -> {
            seenKeys.add(e.getKey());
            return e;
         });
         Flowable<MarshallableEntry<K, V>> flowable = Flowable.fromPublisher(persistenceManager.publishEntries(
               k -> !seenKeys.contains(k), true, true, PersistenceManager.AccessMode.BOTH));
         Publisher<CacheEntry<K, V>> publisher = flowable
               .map(me -> PersistenceUtil.convert(me, iceFactory));
         // This way we don't subscribe to the flowable until after the first iterator is fully exhausted
         return new LazyConcatIterator<>(localIterator, () -> org.infinispan.util.Closeables.iterator(publisher, 128));
      }

      @Override
      protected AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> supplier() {
         return new EntryStreamSupplier<>(cache, partitioner, () -> StreamSupport.stream(spliterator(),
                                                                                         false));
      }

      @Override
      public boolean contains(Object o) {
         boolean contains = false;
         if (o != null) {
            contains = cacheSet.contains(o);
            if (!contains) {
               Map.Entry<K, V> entry = toEntry(o);
               if (entry != null) {
                  MarshallableEntry<K, V> me = CompletionStages.join(persistenceManager.loadFromAllStores(entry.getKey(), true, true));
                  if (me != null) {
                     contains = entry.getValue().equals(me.getValue());
                  }
               }
            }
         }
         return contains;
      }

      @Override
      protected CacheStream<CacheEntry<K, V>> getStream(boolean parallel) {
         return new LocalCacheStream<>(new PersistenceEntryStreamSupplier<>(cache, iceFactory, partitioner::getSegment,
               cacheSet.stream(), persistenceManager), parallel, cache.getAdvancedCache().getComponentRegistry());
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(int segment) {
         Publisher<CacheEntry<K, V>> inMemorySource = cacheSet.localPublisher(segment);
         IntSet segments = IntSets.immutableSet(segment);
         return getCacheEntryPublisher(inMemorySource, segments);
      }

      @Override
      public Publisher<CacheEntry<K, V>> localPublisher(IntSet segments) {
         Publisher<CacheEntry<K, V>> inMemorySource = cacheSet.localPublisher(segments);
         return getCacheEntryPublisher(inMemorySource, segments);
      }

      private Publisher<CacheEntry<K, V>> getCacheEntryPublisher(Publisher<CacheEntry<K, V>> inMemorySource,
                                                                 IntSet segments) {
         Set<K> seenKeys = new HashSet<>(dataContainer.sizeIncludingExpired(segments));
         Publisher<MarshallableEntry<K, V>> loaderSource =
               persistenceManager.publishEntries(segments, k -> !seenKeys.contains(k), true, true, BOTH);
         return Flowable.concat(
               Flowable.fromPublisher(inMemorySource)
                     .doOnNext(ce -> seenKeys.add(ce.getKey())),
               Flowable.fromPublisher(loaderSource)
                     .map(me -> PersistenceUtil.convert(me, iceFactory)));
      }
   }

   private class WrappedKeySet extends AbstractLoaderSet<K> implements CacheSet<K> {

      private final Cache<K, ?> cache;
      private final boolean isRemoteIteration;

      public WrappedKeySet(KeySetCommand command, boolean isRemoteIteration, CacheSet<K> keySet) {
         super(keySet, command.getFlagsBitSet());
         this.cache = Caches.getCacheWithFlags(CacheLoaderInterceptor.this.cache.wired(), command);
         this.isRemoteIteration = isRemoteIteration;
      }

      @Override
      public boolean remove(Object o) {
         // Remove must be done by the cache
         return o != null && cache.remove(o) != null;
      }

      @Override
      public CloseableIterator<K> iterator() {
         if (isRemoteIteration) {
            return innerIterator();
         }
         // Need to support remove of iterator
         return new RemovableCloseableIterator<>(innerIterator(), cache::remove);
      }

      @Override
      protected CloseableIterator<K> innerIterator() {
         // This can be a HashSet since it is only written to from the local iterator which is only invoked
         // from user thread
         Set<K> seenKeys = new HashSet<>(cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
         CloseableIterator<K> localIterator = new IteratorMapper<>(cacheSet.iterator(), k -> {
            seenKeys.add(k);
            return k;
         });
         Flowable<K> flowable = Flowable.fromPublisher(persistenceManager.publishKeys(
               k -> !seenKeys.contains(k), PersistenceManager.AccessMode.BOTH));
         // This way we don't subscribe to the flowable until after the first iterator is fully exhausted
         return new LazyConcatIterator<>(localIterator, () -> org.infinispan.util.Closeables.iterator(flowable, 128));
      }

      @Override
      protected AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> supplier() {
         return new KeyStreamSupplier<>(cache, partitioner, () -> StreamSupport.stream(spliterator(), false));
      }

      @Override
      public boolean contains(Object o) {
         boolean contains = false;
         if (o != null) {
            contains = cacheSet.contains(o);
            if (!contains) {
               MarshallableEntry<K, V> me = CompletionStages.join(persistenceManager.loadFromAllStores(o, true, true));
               contains = me != null;
            }
         }
         return contains;
      }

      @Override
      protected CacheStream<K> getStream(boolean parallel) {
         return new LocalCacheStream<>(new PersistenceKeyStreamSupplier<>(cache, partitioner::getSegment,
               cacheSet.stream(), persistenceManager), parallel, cache.getAdvancedCache().getComponentRegistry());
      }


      @Override
      public Publisher<K> localPublisher(int segment) {
         Publisher<K> inMemorySource = cacheSet.localPublisher(segment);
         IntSet segments = IntSets.immutableSet(segment);
         return getKeyPublisher(inMemorySource, segments);
      }

      @Override
      public Publisher<K> localPublisher(IntSet segments) {
         Publisher<K> inMemorySource = cacheSet.localPublisher(segments);
         return getKeyPublisher(inMemorySource, segments);
      }

      private Publisher<K> getKeyPublisher(Publisher<K> inMemorySource, IntSet segments) {
         Set<K> seenKeys = new HashSet<>(dataContainer.sizeIncludingExpired(segments));
         return Flowable.concat(
               Flowable.fromPublisher(inMemorySource)
                     .doOnNext(seenKeys::add),
               persistenceManager.publishKeys(segments, k -> !seenKeys.contains(k), BOTH));
      }
   }
}
