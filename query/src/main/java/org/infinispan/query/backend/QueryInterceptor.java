package org.infinispan.query.backend;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commands.FlagAffectedCommand;
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
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.query.Transformer;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
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
   private static final Log log = LogFactory.getLog(QueryInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   static final Object UNKNOWN = new Object() {
      @Override
      public String toString() {
         return "<UNKNOWN>";
      }
   };

   @Inject private DistributionManager distributionManager;
   @Inject private RpcManager rpcManager;
   @Inject @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR)
   private ExecutorService asyncExecutor;
   @Inject private InternalCacheRegistry internalCacheRegistry;

   private final IndexModificationStrategy indexingMode;
   private final SearchIntegrator searchFactory;
   private final KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler();
   private final AtomicBoolean stopping = new AtomicBoolean(false);
   private final ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues;
   private QueryKnownClasses queryKnownClasses;
   private SearchWorkCreator searchWorkCreator = new DefaultSearchWorkCreator();
   private SearchFactoryHandler searchFactoryHandler;
   private final DataConversion valueDataConversion;
   private final DataConversion keyDataConversion;
   private boolean isPersistenceEnabled;

   /**
    * The classes declared by the indexing config as indexable. In 8.2 this can be null, indicating that no classes
    * were declared and we are running in the (deprecated) autodetect mode. Autodetect mode will be removed in 9.0.
    */
   private Class<?>[] indexedEntities;

   private final AdvancedCache<?, ?> cache;

   private final InvocationSuccessAction processClearCommand = this::processClearCommand;

   public QueryInterceptor(SearchIntegrator searchFactory, IndexModificationStrategy indexingMode, ConcurrentMap<GlobalTransaction, Map<Object, Object>> txOldValues, AdvancedCache<?, ?> cache) {
      this.searchFactory = searchFactory;
      this.indexingMode = indexingMode;
      this.txOldValues = txOldValues;
      this.cache = cache;
      this.valueDataConversion = cache.getValueDataConversion();
      this.keyDataConversion = cache.getKeyDataConversion();
   }

   @Start
   protected void start() {
      Set<Class<?>> indexedEntities = cacheConfiguration.indexing().indexedEntities();
      this.indexedEntities = indexedEntities.isEmpty() ? null : indexedEntities.toArray(new Class<?>[indexedEntities.size()]);
      queryKnownClasses = indexedEntities.isEmpty() ? new QueryKnownClasses(cache.getName(), cache.getCacheManager(), internalCacheRegistry) : new QueryKnownClasses(indexedEntities);
      searchFactoryHandler = new SearchFactoryHandler(searchFactory, queryKnownClasses, new TransactionHelper(cache.getTransactionManager()));
      if (this.indexedEntities == null) {
         queryKnownClasses.start(searchFactoryHandler);
         Set<Class<?>> classes = queryKnownClasses.keys();
         Class<?>[] classesArray = classes.toArray(new Class<?>[classes.size()]);
         //Important to enable them all in a single call, much more efficient:
         enableClasses(classesArray);
      }
      isPersistenceEnabled = cacheConfiguration.persistence().usingStores();
      stopping.set(false);
   }

   @Stop
   protected void stop() {
      queryKnownClasses.stop();
   }

   public void prepareForStopping() {
      stopping.set(true);
   }

   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx, Object key) {
      return indexingMode.shouldModifyIndexes(command, ctx, distributionManager, rpcManager, key);
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
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            DataWriteCommand cmd = (DataWriteCommand) rCommand;
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

   protected Object handleManyWriteCommand(InvocationContext ctx, WriteCommand command) {
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
         Map<Object, Object> oldValues = new HashMap();
         for (Object key : command.getAffectedKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry != null && (entry.getValue() != null || !unreliablePreviousValue(command))) {
               oldValues.put(key, entry.getValue());
            }
         }
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            WriteCommand cmd = (WriteCommand) rCommand;
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
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(final InvocationContext ctx, final ClearCommand command)
         throws Throwable {
      return invokeNextThenAccept(ctx, command, processClearCommand);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleManyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
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
    * Remove entries from all indexes by key
    */
   void removeFromIndexes(TransactionContext transactionContext, Object key) {
      Stream<IndexedTypeIdentifier> typeIdentifiers = getKnownClasses().stream()
            .filter(searchFactoryHandler::hasIndex)
            .map(PojoIndexedTypeIdentifier::new);
      Set<Work> deleteWorks = typeIdentifiers
            .map(e -> searchWorkCreator.createEntityWork(keyToString(key), e, WorkType.DELETE))
            .collect(Collectors.toSet());
      performSearchWorks(deleteWorks, transactionContext);
   }

   public void purgeIndex(TransactionContext transactionContext, Class<?> entityType) {
      Boolean isIndexable = queryKnownClasses.get(entityType);
      if (isIndexable != null && isIndexable.booleanValue()) {
         if (searchFactoryHandler.hasIndex(entityType)) {
            IndexedTypeIdentifier type = new PojoIndexedTypeIdentifier(entityType);
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks(type, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   private void purgeAllIndexes(TransactionContext transactionContext) {
      for (Class c : queryKnownClasses.keys()) {
         if (searchFactoryHandler.hasIndex(c)) {
            //noinspection unchecked
            IndexedTypeIdentifier type = new PojoIndexedTypeIdentifier(c);
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks(type, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   // Method that will be called when data needs to be removed from Lucene.
   protected void removeFromIndexes(final Object value, final Object key, final TransactionContext transactionContext) {
      performSearchWork(value, keyToString(key), WorkType.DELETE, transactionContext);
   }


   protected void updateIndexes(final boolean usingSkipIndexCleanupFlag, final Object value, final Object key,
                                final TransactionContext transactionContext) {
      // Note: it's generally unsafe to assume there is no previous entry to cleanup: always use UPDATE
      // unless the specific flag is allowing this.
      performSearchWork(value, keyToString(key), usingSkipIndexCleanupFlag ? WorkType.ADD : WorkType.UPDATE, transactionContext);
   }

   private void performSearchWork(Object value, Serializable id, WorkType workType,
                                  TransactionContext transactionContext) {
      if (value == null) throw new NullPointerException("Cannot handle a null value!");
      Collection<Work> works = searchWorkCreator.createPerEntityWorks(value, id, workType);
      performSearchWorks(works, transactionContext);
   }

   private void performSearchWorks(Collection<Work> works, TransactionContext transactionContext) {
      Worker worker = searchFactory.getWorker();
      for (Work work : works) {
         worker.performWork(work, transactionContext);
      }
   }

   /**
    * The set of known classes. Some might be indexable, some are not.
    *
    * @return an immutable set
    */
   public Set<Class<?>> getKnownClasses() {
      return queryKnownClasses.keys();
   }

   private Object extractValue(Object storedValue) {
      return valueDataConversion.extractIndexable(storedValue);
   }

   private Object extractKey(Object storedKey) {
      return keyDataConversion.extractIndexable(storedKey);
   }

   public void enableClasses(Class[] classes) {
      searchFactoryHandler.enableClasses(classes);
   }

   public boolean updateKnownTypesIfNeeded(Object value) {
      return searchFactoryHandler.updateKnownTypesIfNeeded(value);
   }

   public void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      keyTransformationHandler.registerTransformer(keyClass, transformerClass);
   }

   private String keyToString(Object key) {
      return keyTransformationHandler.keyToString(extractKey(key));
   }

   public KeyTransformationHandler getKeyTransformationHandler() {
      return keyTransformationHandler;
   }

   public SearchIntegrator getSearchFactory() {
      return searchFactory;
   }

   /**
    * Customize work creation during indexing
    *
    * @param searchWorkCreator custom {@link org.infinispan.query.backend.SearchWorkCreator}
    */
   public void setSearchWorkCreator(SearchWorkCreator searchWorkCreator) {
      this.searchWorkCreator = searchWorkCreator;
   }

   public SearchWorkCreator getSearchWorkCreator() {
      return searchWorkCreator;
   }

   void processChange(InvocationContext ctx, FlagAffectedCommand command, Object storedKey, Object storedOldValue, Object storedNewValue, TransactionContext transactionContext) {
      Object key = extractKey(storedKey);
      Object oldValue = storedOldValue == UNKNOWN ? UNKNOWN : extractValue(storedOldValue);
      Object newValue = extractValue(storedNewValue);
      boolean skipIndexCleanup = command != null && command.hasAnyFlag(FlagBitSets.SKIP_INDEX_CLEANUP);
      if (!skipIndexCleanup) {
         if (oldValue == UNKNOWN) {
            if (shouldModifyIndexes(command, ctx, storedKey)) {
               removeFromIndexes(transactionContext, key);
            }
         } else if (updateKnownTypesIfNeeded(oldValue) && (newValue == null || shouldRemove(newValue, oldValue))
               && shouldModifyIndexes(command, ctx, storedKey)) {
            removeFromIndexes(oldValue, key, transactionContext);
         } else if (trace) {
            log.tracef("Index cleanup not needed for %s -> %s", oldValue, newValue);
         }
      } else if (trace) {
         log.tracef("Skipped index cleanup for command %s", command);
      }
      if (updateKnownTypesIfNeeded(newValue)) {
         if (shouldModifyIndexes(command, ctx, storedKey)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            updateIndexes(skipIndexCleanup, newValue, key, transactionContext);
         } else {
            log.tracef("Not modifying index for %s (%s)", storedKey, command);
         }
      } else if (trace) {
         log.tracef("Update not needed for %s", newValue);
      }
   }

   private boolean shouldRemove(Object value, Object previousValue) {
      if (getSearchWorkCreator() instanceof ExtendedSearchWorkCreator) {
         ExtendedSearchWorkCreator eswc = ExtendedSearchWorkCreator.class.cast(getSearchWorkCreator());
         return eswc.shouldRemove(new SearchWorkCreatorContext(previousValue, value));
      } else {
         return !(value == null || previousValue == null) && !value.getClass().equals(previousValue.getClass());
      }
   }

   private void processClearCommand(InvocationContext ctx, VisitableCommand command, Object rv) {
      if (shouldModifyIndexes((ClearCommand) command, ctx, null)) {
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
