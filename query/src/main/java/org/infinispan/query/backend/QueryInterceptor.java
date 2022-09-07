package org.infinispan.query.backend;

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager.StoreChangeListener;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.logging.Log;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;

/**
 * This interceptor will be created when the System Property "infinispan.query.indexLocalOnly" is "false"
 * <p>
 * This type of interceptor will allow the indexing of data even when it comes from other caches within a cluster.
 * <p>
 * However, if the a cache would not be putting the data locally, the interceptor will not index it.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @author anistor@redhat.com
 * @since 4.0
 */
public final class QueryInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(QueryInterceptor.class, Log.class);

   static final Object UNKNOWN = new Object() {
      @Override
      public String toString() {
         return "<UNKNOWN>";
      }
   };

   @Inject
   DistributionManager distributionManager;
   @Inject
   BlockingManager blockingManager;
   @Inject
   protected KeyPartitioner keyPartitioner;
   @Inject
   protected PersistenceManager persistenceManager;

   private final AtomicBoolean stopping = new AtomicBoolean(false);
   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;
   private final DataConversion valueDataConversion;
   private final DataConversion keyDataConversion;
   private volatile boolean isPersistenceEnabled;

   private final InvocationSuccessAction<ClearCommand> processClearCommand = this::processClearCommand;
   private final boolean isManualIndexing;
   private final AdvancedCache<?, ?> cache;
   private final Map<String, Class<?>> indexedClasses;

   private SearchMapping searchMapping;
   private SegmentListener segmentListener;
   private final StoreChangeListener storeChangeListener = pm -> isPersistenceEnabled = pm.isEnabled();

   public QueryInterceptor(boolean isManualIndexing, ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues,
                           AdvancedCache<?, ?> cache, Map<String, Class<?>> indexedClasses) {
      this.isManualIndexing = isManualIndexing;
      this.txOldValues = txOldValues;
      this.valueDataConversion = cache.getValueDataConversion();
      this.keyDataConversion = cache.getKeyDataConversion();
      this.isPersistenceEnabled = cache.getCacheConfiguration().persistence().usingStores();
      this.cache = cache;
      this.indexedClasses = Collections.unmodifiableMap(indexedClasses);
   }

   @Start
   protected void start() {
      stopping.set(false);
      boolean isClustered = cache.getCacheConfiguration().clustering().cacheMode().isClustered();
      if (isClustered) {
         segmentListener = new SegmentListener(cache, this::purgeIndex, blockingManager);
         this.cache.addListener(segmentListener);
      }
      searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      persistenceManager.addStoreListener(storeChangeListener);
   }

   @Stop
   protected void stop() {
      persistenceManager.removeStoreListener(storeChangeListener);
   }

   public void prepareForStopping() {
      if (segmentListener != null) cache.removeListener(segmentListener);
      stopping.set(true);
   }

   private boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx, Object key) {
      if (isManualIndexing) return false;

      if (distributionManager == null || key == null) {
         return true;
      }

      DistributionInfo info = distributionManager.getCacheTopology().getDistribution(key);
      // If this is a backup node we should modify the entry in the remote context
      return info.isPrimary() || info.isWriteOwner() &&
            (ctx.isInTxScope() || !ctx.isOriginLocal() || command != null && command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER));
   }

   public BlockingManager getBlockingManager() {
      return blockingManager;
   }

   private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) {
         return invokeNext(ctx, command);
      }
      // Make sure the searchMapping is ready to accept requests before allowing the invocation to proceed further
      if (searchMapping.isClose()) {
         throw log.searchMappingUnavailable();
      }

      return invokeNextThenApply(ctx, command, (rCtx, cmd, rv) -> {
         if (!cmd.isSuccessful()) {
            return rv;
         }
         boolean unreliablePrevious = unreliablePreviousValue(cmd);
         if (rCtx.isInTxScope()) {
            Map<Object, Object> oldValues = getOldValuesMap((TxInvocationContext<?>) rCtx);
            registerOldValue(rCtx, cmd.getKey(), unreliablePrevious, oldValues);
         } else {
            return delayedValue(indexIfNeeded(rCtx, cmd, unreliablePrevious, cmd.getKey()), rv);
         }
         return rv;
      });
   }

   private Object handleManyWriteCommand(InvocationContext ctx, WriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, (rCtx, cmd, rv) -> {
         if (!cmd.isSuccessful()) {
            return rv;
         }
         boolean unreliablePrevious = unreliablePreviousValue(cmd);
         if (rCtx.isInTxScope()) {
            Map<Object, Object> oldValues = getOldValuesMap((TxInvocationContext<?>) rCtx);
            for (Object key : cmd.getAffectedKeys()) {
               registerOldValue(rCtx, key, unreliablePrevious, oldValues);
            }
            return rv;
         } else {
            return delayedValue(allOf(cmd.getAffectedKeys().stream().map(key -> indexIfNeeded(rCtx, cmd, unreliablePrevious, key)).toArray(CompletableFuture[]::new)), rv);
         }
      });
   }

   private void registerOldValue(InvocationContext ctx, Object key, boolean unreliablePrevious, Map<Object, Object> oldValues) {
      CacheEntry<?, ?> entryTx = ctx.lookupEntry(key);
      if (entryTx != null && (entryTx.getValue() != null || !unreliablePrevious)) {
         ReadCommittedEntry<?, ?> mvccEntry = (ReadCommittedEntry<?, ?>) entryTx;
         oldValues.putIfAbsent(key, mvccEntry.getOldValue());
      }
   }

   private Map<Object, Object> getOldValuesMap(TxInvocationContext<?> ctx) {
      return txOldValues.computeIfAbsent(ctx.getGlobalTransaction(), gid -> {
         ctx.getCacheTransaction().addListener(() -> txOldValues.remove(gid));
         return new HashMap<>();
      });
   }

   private CompletableFuture<?> indexIfNeeded(InvocationContext rCtx, WriteCommand cmd, boolean unreliablePrevious, Object key) {
      CacheEntry<?, ?> entry = rCtx.lookupEntry(key);
      boolean isStale = false;
      Object old = null;
      if (entry instanceof MVCCEntry) {
         ReadCommittedEntry<?, ?> mvccEntry = (ReadCommittedEntry<?, ?>) entry;
         isStale = !mvccEntry.isCommitted();
         old = unreliablePrevious ? UNKNOWN : mvccEntry.getOldValue();
      }
      if (entry != null && entry.isChanged() && !isStale) {
         if (log.isDebugEnabled()) {
            log.debugf("Try indexing command '%s',key='%s', oldValue='%s', stale='false'", cmd, key, old);
         }
         return processChange(rCtx, cmd, key, old, entry.getValue());
      } else {
         if (log.isDebugEnabled()) {
            log.debugf("Skipping indexing for command '%s',key='%s', oldValue='%s', stale='%s'", cmd, key, old, isStale);
         }
      }
      return CompletableFutures.completedNull();
   }

   private boolean unreliablePreviousValue(WriteCommand command) {
      // alternative approach would be changing the flag and forcing load type in an interceptor before EWI
      return isPersistenceEnabled && (command.loadType() == VisitableCommand.LoadType.DONT_LOAD
            || command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD));
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      return invokeNextThenAccept(ctx, command, processClearCommand);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return handleManyWriteCommand(ctx, command);
   }

   /**
    * Remove all entries from all known indexes
    */
   public void purgeAllIndexes() {
      if (searchMapping == null) {
         return;
      }

      searchMapping.scopeAll().workspace().purge();
   }

   public void purgeIndex(Class<?> entityType) {
      if (searchMapping == null) {
         return;
      }

      searchMapping.scope(entityType).workspace().purge();
   }

   /**
    * Removes from the index the entries corresponding to the supplied segments, if the index is local.
    */
   void purgeIndex(IntSet segments) {
      if (segments == null || segments.isEmpty() || searchMapping == null) {
         return;
      }

      Set<String> routingKeys = segments.intStream().boxed().map(Objects::toString).collect(Collectors.toSet());
      searchMapping.scopeAll().workspace().purge(routingKeys);
   }

   /**
    * Remove entries from all indexes by key.
    */
   CompletableFuture<?> removeFromIndexes(Object key, int segment) {
      return getSearchIndexer().purge(key, String.valueOf(segment));
   }

   // Method that will be called when data needs to be removed from Lucene.
   private CompletableFuture<?> removeFromIndexes(Object value, Object key, int segment) {
      return getSearchIndexer().delete(key, String.valueOf(segment), value);
   }

   private CompletableFuture<?> updateIndexes(boolean usingSkipIndexCleanupFlag, Object value, Object key, int segment) {
      // Note: it's generally unsafe to assume there is no previous entry to cleanup: always use UPDATE
      // unless the specific flag is allowing this.
      if (usingSkipIndexCleanupFlag) {
         return getSearchIndexer().add(key, String.valueOf(segment), value);
      } else {
         return getSearchIndexer().addOrUpdate(key, String.valueOf(segment), value);
      }
   }

   /**
    * The indexed classes.
    *
    * @deprecated since 11
    */
   @Deprecated
   public Map<String, Class<?>> indexedEntities() {
      return indexedClasses;
   }

   private SearchIndexer getSearchIndexer() {
      return searchMapping.getSearchIndexer();
   }

   private Object extractValue(Object storedValue) {
      return valueDataConversion.extractIndexable(storedValue);
   }

   private Object extractKey(Object storedKey) {
      return keyDataConversion.extractIndexable(storedKey);
   }

   CompletableFuture<?> processChange(InvocationContext ctx, FlagAffectedCommand command, Object storedKey, Object storedOldValue, Object storedNewValue) {
      if (searchMapping.isRestarting()) {
         log.mappingIsRestarting();
         return CompletableFutures.completedNull();
      }

      int segment = SegmentSpecificCommand.extractSegment(command, storedKey, keyPartitioner);
      Object key = extractKey(storedKey);
      Object oldValue = storedOldValue == UNKNOWN ? UNKNOWN : extractValue(storedOldValue);
      Object newValue = extractValue(storedNewValue);
      boolean skipIndexCleanup = command != null && command.hasAnyFlag(FlagBitSets.SKIP_INDEX_CLEANUP);
      CompletableFuture<?> operation = CompletableFutures.completedNull();
      if (!skipIndexCleanup) {
         if (oldValue == UNKNOWN) {
            if (shouldModifyIndexes(command, ctx, storedKey)) {
               operation = removeFromIndexes(key, segment);
            }
         } else if (isPotentiallyIndexedType(oldValue) && (newValue == null || replacedWithADifferentEntity(newValue, oldValue))
               && shouldModifyIndexes(command, ctx, storedKey)) {
            operation = removeFromIndexes(oldValue, key, segment);
         } else if (log.isTraceEnabled()) {
            log.tracef("Index cleanup not needed for %s -> %s", oldValue, newValue);
         }
      } else if (log.isTraceEnabled()) {
         log.tracef("Skipped index cleanup for command %s", command);
      }
      if (isPotentiallyIndexedType(newValue)) {
         if (shouldModifyIndexes(command, ctx, storedKey)) {
            // We don't need to wait for a possible removeFromIndexes operation,
            // since if it exists, the oldValue is UNKNOWN or replacedWithADifferentEntity is true,
            // which implies that the delete and the add operations are related to different indexes
            operation = CompletableFuture.allOf(operation, updateIndexes(skipIndexCleanup, newValue, key, segment));
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Not modifying index for %s (%s)", storedKey, command);
            }
         }
      } else if (log.isTraceEnabled()) {
         log.tracef("Update not needed for %s", newValue);
      }
      return operation;
   }

   private boolean replacedWithADifferentEntity(Object value, Object previousValue) {
      return value != null && previousValue != null && value.getClass() != previousValue.getClass();
   }

   private void processClearCommand(InvocationContext ctx, ClearCommand command, Object rv) {
      if (shouldModifyIndexes(command, ctx, null)) {
         purgeAllIndexes();
      }
   }

   public boolean isStopping() {
      return stopping.get();
   }

   /**
    * @param value An entity.
    * @return {@code true} if there is a chance that this entity is of an indexed types.
    * For protobuf entities which are not yet deserialized,
    * this returns {@code true} even though we don't know the exact type until the entity is deserialized.
    * The {@link org.infinispan.search.mapper.mapping.EntityConverter entity converter}
    * that takes care of deserialization will take care of cancelling indexing
    * if it turns out the actual type of the entity is not one that should be indexed.
    */
   private boolean isPotentiallyIndexedType(Object value) {
      if (searchMapping == null) {
         return false;
      }
      Class<?> convertedType = searchMapping.toConvertedEntityJavaClass(value);
      return searchMapping.allIndexedEntityJavaClasses().contains(convertedType);
   }
}
