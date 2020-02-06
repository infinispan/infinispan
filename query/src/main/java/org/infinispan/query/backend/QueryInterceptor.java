package org.infinispan.query.backend;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexingMode;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
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
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.query.impl.IndexInspector;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.xa.GlobalTransaction;
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
   private static final boolean trace = log.isTraceEnabled();

   static final Object UNKNOWN = new Object() {
      @Override
      public String toString() {
         return "<UNKNOWN>";
      }
   };

   @Inject DistributionManager distributionManager;
   @Inject RpcManager rpcManager;
   @Inject @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR)
   ExecutorService asyncExecutor;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject IndexInspector indexInspector;

   private final IndexModificationStrategy indexingMode;
   private final SearchIntegrator searchFactory;
   private final KeyTransformationHandler keyTransformationHandler;
   private final AtomicBoolean stopping = new AtomicBoolean(false);
   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;
   private SearchWorkCreator searchWorkCreator = SearchWorkCreator.DEFAULT;
   private final DataConversion valueDataConversion;
   private final DataConversion keyDataConversion;
   private final boolean isPersistenceEnabled;

   private final InvocationSuccessAction<ClearCommand> processClearCommand = this::processClearCommand;
   private final boolean isManualIndexing;

   public QueryInterceptor(SearchIntegrator searchFactory, KeyTransformationHandler keyTransformationHandler,
                           IndexModificationStrategy indexingMode,
                           ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues,
                           AdvancedCache<?, ?> cache) {
      this.searchFactory = searchFactory;
      this.keyTransformationHandler = keyTransformationHandler;
      this.indexingMode = indexingMode;
      this.isManualIndexing = searchFactory.unwrap(SearchIntegrator.class).getIndexingMode() == IndexingMode.MANUAL;
      this.txOldValues = txOldValues;
      this.valueDataConversion = cache.getValueDataConversion();
      this.keyDataConversion = cache.getKeyDataConversion();
      Configuration cfg = cache.getCacheConfiguration();
      this.isPersistenceEnabled = cfg.persistence().usingStores();
   }

   @Start
   protected void start() {
      stopping.set(false);
   }

   public void prepareForStopping() {
      stopping.set(true);
   }

   private boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx, Object key, Object value) {
      if(indexingMode != null) {
         return indexingMode.shouldModifyIndexes(command, ctx, distributionManager, rpcManager, key);
      }

      if (isManualIndexing) return false;

      IndexModificationStrategy strategy;
      CacheMode cacheMode = cacheConfiguration.clustering().cacheMode();
      boolean clustered = cacheMode.isClustered();

      if (!clustered) return true;

      if (value == null) return cacheMode.isReplicated() || ctx.isOriginLocal();

      strategy = indexInspector.hasSharedIndex(value.getClass()) ? IndexModificationStrategy.PRIMARY_OWNER : IndexModificationStrategy.ALL;

      return strategy.shouldModifyIndexes(command, ctx, distributionManager, rpcManager, key);
   }

   /**
    * Use this executor for Async operations
    */
   public ExecutorService getAsyncExecutor() {
      return asyncExecutor;
   }

   private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) {
         return invokeNext(ctx, command);
      }
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (ctx.isInTxScope()) {
         // replay of modifications on remote node by EntryWrappingVisitor
         if (entry != null && !entry.isChanged() && (entry.getValue() != null || !unreliablePreviousValue(command))) {
            Map<Object, Object> oldValues = registerOldValues((TxInvocationContext) ctx);
            oldValues.putIfAbsent(command.getKey(), entry.getValue());
         }
         return invokeNext(ctx, command);
      } else {
         Object prev = entry != null ? entry.getValue() : UNKNOWN;
         if (prev == null && unreliablePreviousValue(command)) {
            prev = UNKNOWN;
         }
         Object oldValue = prev;
         return invokeNextThenAccept(ctx, command, (rCtx, cmd, rv) -> {
            if (!cmd.isSuccessful()) {
               return;
            }
            CacheEntry entry2 = entry != null ? entry : rCtx.lookupEntry(cmd.getKey());
            if (entry2 != null && entry2.isChanged()) {
               processChange(rCtx, cmd, cmd.getKey(), oldValue, entry2.getValue(), NoTransactionContext.INSTANCE);
            }
         });
      }
   }

   private Map<Object, Object> registerOldValues(TxInvocationContext ctx) {
      return txOldValues.computeIfAbsent(ctx.getGlobalTransaction(), gid -> {
         ctx.getCacheTransaction().addListener(() -> txOldValues.remove(gid));
         return new HashMap<>();
      });
   }

   private Object handleManyWriteCommand(InvocationContext ctx, WriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) {
         return invokeNext(ctx, command);
      }
      if (ctx.isInTxScope()) {
         Map<Object, Object> oldValues = registerOldValues((TxInvocationContext) ctx);
         for (Object key : command.getAffectedKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry != null && !entry.isChanged() && (entry.getValue() != null || !unreliablePreviousValue(command))) {
               oldValues.putIfAbsent(key, entry.getValue());
            }
         }
         return invokeNext(ctx, command);
      } else {
         Map<Object, Object> oldValues = new HashMap<>();
         for (Object key : command.getAffectedKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry != null && (entry.getValue() != null || !unreliablePreviousValue(command))) {
               oldValues.put(key, entry.getValue());
            }
         }
         return invokeNextThenAccept(ctx, command, (rCtx, cmd, rv) -> {
            if (!cmd.isSuccessful()) {
               return;
            }
            for (Object key : cmd.getAffectedKeys()) {
               CacheEntry entry = rCtx.lookupEntry(key);
               // If the entry is null we are not an owner and won't index it
               if (entry != null && entry.isChanged()) {
                  Object oldValue = oldValues.getOrDefault(key, UNKNOWN);
                  processChange(rCtx, cmd, key, oldValue, entry.getValue(), NoTransactionContext.INSTANCE);
               }
            }
         });
      }
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
      purgeAllIndexes(NoTransactionContext.INSTANCE);
   }

   public void purgeIndex(Class<?> entityType) {
      purgeIndex(NoTransactionContext.INSTANCE, entityType);
   }

   /**
    * Remove entries from all indexes by key.
    */
   void removeFromIndexes(TransactionContext transactionContext, Object key, int segment) {
      for (IndexedTypeIdentifier type : searchFactory.getIndexBindings().keySet()) {
         performSearchWork(searchWorkCreator.createPerEntityWork(keyToString(key, segment), type, WorkType.DELETE), transactionContext);
      }
   }

   private void purgeIndex(TransactionContext transactionContext, Class<?> entityType) {
      IndexedTypeIdentifier type = new PojoIndexedTypeIdentifier(entityType);
      if (searchFactory.getIndexBindings().containsKey(type)) {
         performSearchWork(searchWorkCreator.createPerEntityTypeWork(type, WorkType.PURGE_ALL), transactionContext);
      }
   }

   private void purgeAllIndexes(TransactionContext transactionContext) {
      for (IndexedTypeIdentifier type : searchFactory.getIndexBindings().keySet()) {
         performSearchWork(searchWorkCreator.createPerEntityTypeWork(type, WorkType.PURGE_ALL), transactionContext);
      }
   }

   // Method that will be called when data needs to be removed from Lucene.
   private void removeFromIndexes(Object value, Object key, TransactionContext transactionContext, int segment) {
      performSearchWork(value, keyToString(key, segment), WorkType.DELETE, transactionContext);
   }

   private void updateIndexes(boolean usingSkipIndexCleanupFlag, Object value, Object key, TransactionContext transactionContext, int segment) {
      // Note: it's generally unsafe to assume there is no previous entry to cleanup: always use UPDATE
      // unless the specific flag is allowing this.
      performSearchWork(value, keyToString(key, segment), usingSkipIndexCleanupFlag ? WorkType.ADD : WorkType.UPDATE, transactionContext);
   }

   private void performSearchWork(Object value, Serializable id, WorkType workType, TransactionContext transactionContext) {
      if (value == null) throw new NullPointerException("Cannot handle a null value!");
      performSearchWork(searchWorkCreator.createPerEntityWork(value, id, workType), transactionContext);
   }

   private void performSearchWork(Work work, TransactionContext transactionContext) {
      if (work != null) {
         Worker worker = searchFactory.getWorker();
         worker.performWork(work, transactionContext);
      }
   }

   /**
    * The indexed classes.
    */
   public Map<String, Class<?>> indexedEntities() {
      return indexInspector.getIndexedEntities();
   }

   private Object extractValue(Object storedValue) {
      return valueDataConversion.extractIndexable(storedValue);
   }

   private Object extractKey(Object storedKey) {
      return keyDataConversion.extractIndexable(storedKey);
   }

   private String keyToString(Object key, int segment) {
      return keyTransformationHandler.keyToString(extractKey(key), segment);
   }

   public KeyTransformationHandler getKeyTransformationHandler() {
      return keyTransformationHandler;
   }

   /**
    * Get the search work creator.
    */
   public SearchWorkCreator getSearchWorkCreator() {
      return searchWorkCreator;
   }

   /**
    * Customize work creation during indexing
    *
    * @param searchWorkCreator custom {@link org.infinispan.query.backend.SearchWorkCreator}
    */
   public void setSearchWorkCreator(SearchWorkCreator searchWorkCreator) {
      this.searchWorkCreator = searchWorkCreator;
   }

   void processChange(InvocationContext ctx, FlagAffectedCommand command, Object storedKey, Object storedOldValue, Object storedNewValue, TransactionContext transactionContext) {
      int segment = SegmentSpecificCommand.extractSegment(command, storedKey, keyPartitioner);
      Object key = extractKey(storedKey);
      Object oldValue = storedOldValue == UNKNOWN ? UNKNOWN : extractValue(storedOldValue);
      Object newValue = extractValue(storedNewValue);
      boolean skipIndexCleanup = command != null && command.hasAnyFlag(FlagBitSets.SKIP_INDEX_CLEANUP);
      if (!skipIndexCleanup) {
         if (oldValue == UNKNOWN) {
            if (shouldModifyIndexes(command, ctx, storedKey, newValue)) {
               removeFromIndexes(transactionContext, key, segment);
            }
         } else if (indexInspector.isIndexedType(oldValue) && (newValue == null || shouldRemove(newValue, oldValue))
               && shouldModifyIndexes(command, ctx, storedKey, oldValue)) {
            removeFromIndexes(oldValue, key, transactionContext, segment);
         } else if (trace) {
            log.tracef("Index cleanup not needed for %s -> %s", oldValue, newValue);
         }
      } else if (trace) {
         log.tracef("Skipped index cleanup for command %s", command);
      }
      if (indexInspector.isIndexedType(newValue)) {
         if (shouldModifyIndexes(command, ctx, storedKey, newValue)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            updateIndexes(skipIndexCleanup, newValue, key, transactionContext, segment);
         } else {
            if (trace) {
               log.tracef("Not modifying index for %s (%s)", storedKey, command);
            }
         }
      } else if (trace) {
         log.tracef("Update not needed for %s", newValue);
      }
   }

   private boolean shouldRemove(Object value, Object previousValue) {
      return value != null && previousValue != null && value.getClass() != previousValue.getClass();
   }

   private void processClearCommand(InvocationContext ctx, ClearCommand command, Object rv) {
      if (shouldModifyIndexes(command, ctx, null, null)) {
         purgeAllIndexes(NoTransactionContext.INSTANCE);
      }
   }

   public IndexModificationStrategy getIndexModificationMode() {
      return indexingMode;
   }

   public boolean isStopping() {
      return stopping.get();
   }
}
