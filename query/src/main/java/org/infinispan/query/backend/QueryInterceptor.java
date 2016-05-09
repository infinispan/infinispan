package org.infinispan.query.backend;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.store.ShardIdentifierProvider;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.query.Transformer;
import org.infinispan.query.affinity.AffinityShardIdentifierProvider;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.query.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This interceptor will be created when the System Property "infinispan.query.indexLocalOnly" is "false"
 * <p/>
 * This type of interceptor will allow the indexing of data even when it comes from other caches within a cluster.
 * <p/>
 * However, if the a cache would not be putting the data locally, the interceptor will not index it.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @author anistor@redhat.com
 * @since 4.0
 */
public final class QueryInterceptor extends DDAsyncInterceptor {

   private final IndexModificationStrategy indexingMode;
   private final SearchIntegrator searchFactory;
   private final KeyTransformationHandler keyTransformationHandler = new KeyTransformationHandler();
   private final AtomicBoolean stopping = new AtomicBoolean(false);

   private QueryKnownClasses queryKnownClasses;

   private SearchWorkCreator<Object> searchWorkCreator = new DefaultSearchWorkCreator<>();

   private SearchFactoryHandler searchFactoryHandler;

   private DataContainer dataContainer;
   protected TransactionManager transactionManager;
   protected TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   private DistributionManager distributionManager;
   private RpcManager rpcManager;
   protected ExecutorService asyncExecutor;

   private static final Log log = LogFactory.getLog(QueryInterceptor.class, Log.class);

   /**
    * The classes declared by the indexing config as indexable. In 8.2 this can be null, indicating that no classes
    * were declared and we are running in the (deprecated) autodetect mode. Autodetect mode will be removed in 9.0.
    */
   private Class<?>[] indexedEntities;

   public QueryInterceptor(SearchIntegrator searchFactory, IndexModificationStrategy indexingMode) {
      this.searchFactory = searchFactory;
      this.indexingMode = indexingMode;
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(TransactionManager transactionManager,
                                     TransactionSynchronizationRegistry transactionSynchronizationRegistry,
                                     Cache cache,
                                     EmbeddedCacheManager cacheManager,
                                     InternalCacheRegistry internalCacheRegistry,
                                     DistributionManager distributionManager,
                                     RpcManager rpcManager,
                                     DataContainer dataContainer,
                                     @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e) {
      this.transactionManager = transactionManager;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.asyncExecutor = e;
      this.dataContainer = dataContainer;
      Set<Class<?>> indexedEntities = cache.getCacheConfiguration().indexing().indexedEntities();
      this.indexedEntities = indexedEntities.isEmpty() ? null : indexedEntities.toArray(new Class<?>[indexedEntities.size()]);
      this.queryKnownClasses = indexedEntities.isEmpty() ? new QueryKnownClasses(cache.getName(), cacheManager, internalCacheRegistry) : new QueryKnownClasses(indexedEntities);
      this.searchFactoryHandler = new SearchFactoryHandler(this.searchFactory, this.queryKnownClasses, new TransactionHelper(transactionManager));
   }

   @Start
   protected void start() {
      if (indexedEntities == null) {
         queryKnownClasses.start(searchFactoryHandler);
         Set<Class<?>> classes = queryKnownClasses.keys();
         Class<?>[] classesArray = classes.toArray(new Class<?>[classes.size()]);
         //Important to enable them all in a single call, much more efficient:
         enableClasses(classesArray);
      }
      stopping.set(false);
   }

   @Stop
   protected void stop() {
      queryKnownClasses.stop();
   }

   public void prepareForStopping() {
      stopping.set(true);
   }

   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx) {
      return indexingMode.shouldModifyIndexes(command, ctx);
   }

   /**
    * Use this executor for Async operations
    */
   public ExecutorService getAsyncExecutor() {
      return asyncExecutor;
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            processPutKeyValueCommand(((PutKeyValueCommand) rCommand), rCtx, rv, null);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      // remove the object out of the cache first.
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            processRemoveCommand(((RemoveCommand) rCommand), rCtx, rv, null);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            processReplaceCommand(((ReplaceCommand) rCommand), rCtx, rv, null);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            Map<Object, Object> previousValues = (Map<Object, Object>) rv;
            processPutMapCommand(((PutMapCommand) rCommand), rCtx, previousValues, null);
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitClearCommand(final InvocationContext ctx, final ClearCommand command) throws Throwable {
      // This method is called when somebody calls a cache.clear() and we will need to wipe everything in the indexes.
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            processClearCommand(((ClearCommand) rCommand), rCtx, null);
         }
         return null;
      });
   }

   /**
    * Remove all entries from all known indexes
    */
   public void purgeAllIndexes() {
      purgeAllIndexes(null);
   }

   public void purgeIndex(Class<?> entityType) {
      purgeIndex(null, entityType);
   }

   private void purgeIndex(TransactionContext transactionContext, Class<?> entityType) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
      Boolean isIndexable = queryKnownClasses.get(entityType);
      if (isIndexable != null && isIndexable.booleanValue()) {
         if (searchFactoryHandler.hasIndex(entityType)) {
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks((Class<Object>) entityType, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   private void purgeAllIndexes(TransactionContext transactionContext) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
      for (Class c : queryKnownClasses.keys()) {
         if (searchFactoryHandler.hasIndex(c)) {
            //noinspection unchecked
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks(c, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   // Method that will be called when data needs to be removed from Lucene.
   protected void removeFromIndexes(final Object value, final Object key, final TransactionContext transactionContext) {
      performSearchWork(value, keyToString(key), WorkType.DELETE, transactionContext);
   }

   private boolean isPrimaryOwner(Object key) {
      return distributionManager == null || distributionManager.getPrimaryLocation(key).equals(rpcManager.getAddress());
   }

   protected void updateIndexes(final boolean usingSkipIndexCleanupFlag, final Object value, final Object key, final TransactionContext transactionContext) {
      // Note: it's generally unsafe to assume there is no previous entry to cleanup: always use UPDATE
      // unless the specific flag is allowing this.
      ShardIdentifierProvider shardIdentifierProvider = searchFactory.getIndexBinding(value.getClass()).getShardIdentifierProvider();
      if (shardIdentifierProvider == null || !(shardIdentifierProvider instanceof AffinityShardIdentifierProvider) || isPrimaryOwner(key)) {
         performSearchWork(value, keyToString(key), usingSkipIndexCleanupFlag ? WorkType.ADD : WorkType.UPDATE, transactionContext);
      }
   }

   private void performSearchWork(Object value, Serializable id, WorkType workType, TransactionContext transactionContext) {
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

   public boolean hasIndex(final Class<?> c) {
      return searchFactoryHandler.hasIndex(c);
   }

   private Object extractValue(Object wrappedValue) {
      if (wrappedValue instanceof MarshalledValue)
         return ((MarshalledValue) wrappedValue).get();
      else
         return wrappedValue;
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
      return keyTransformationHandler.keyToString(key);
   }

   public KeyTransformationHandler getKeyTransformationHandler() {
      return keyTransformationHandler;
   }

   public SearchIntegrator getSearchFactory() {
      return searchFactory;
   }

   /**
    * Customize work creation during indexing
    * @param searchWorkCreator custom {@link org.infinispan.query.backend.SearchWorkCreator}
    */
   public void setSearchWorkCreator(SearchWorkCreator<Object> searchWorkCreator) {
      this.searchWorkCreator = searchWorkCreator;
   }

   public SearchWorkCreator<Object> getSearchWorkCreator() {
      return searchWorkCreator;
   }

   /**
    * In case of a remotely originating transactions we don't have a chance to visit the single
    * commands but receive this "batch". We then need the before-apply snapshot of some types
    * to route the cleanup commands to the correct indexes.
    * Note we don't need to visit the CommitCommand as the indexing context is registered
    * as a transaction sync.
    */
   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final WriteCommand[] writeCommands = command.getModifications();
      final Object[] stateBeforePrepare = new Object[writeCommands.length];

      for (int i = 0; i < writeCommands.length; i++) {
         final WriteCommand writeCommand = writeCommands[i];
         if (writeCommand instanceof PutKeyValueCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((PutKeyValueCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         } else if (writeCommand instanceof PutMapCommand) {
            stateBeforePrepare[i] = getPreviousValues(((PutMapCommand) writeCommand).getMap().keySet());
         } else if (writeCommand instanceof RemoveCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((RemoveCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         } else if (writeCommand instanceof ReplaceCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((ReplaceCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         }
      }

      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable == null) {
            TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
            if (txInvocationContext.isTransactionValid()) {
               final TransactionContext transactionContext = makeTransactionalEventContext();
               for (int i = 0; i < writeCommands.length; i++) {
                  final WriteCommand writeCommand = writeCommands[i];
                  if (writeCommand instanceof PutKeyValueCommand) {
                     processPutKeyValueCommand((PutKeyValueCommand) writeCommand, txInvocationContext, stateBeforePrepare[i],
                           transactionContext);
                  } else if (writeCommand instanceof PutMapCommand) {
                     processPutMapCommand((PutMapCommand) writeCommand, txInvocationContext,
                           (Map<Object, Object>) stateBeforePrepare[i], transactionContext);
                  } else if (writeCommand instanceof RemoveCommand) {
                     processRemoveCommand((RemoveCommand) writeCommand, txInvocationContext, stateBeforePrepare[i],
                           transactionContext);
                  } else if (writeCommand instanceof ReplaceCommand) {
                     processReplaceCommand((ReplaceCommand) writeCommand, txInvocationContext, stateBeforePrepare[i],
                           transactionContext);
                  } else if (writeCommand instanceof ClearCommand) {
                     processClearCommand((ClearCommand) writeCommand, txInvocationContext, transactionContext);
                  }
               }
            }
         }
         return null;
      });
   }

   private Map<Object, Object> getPreviousValues(Set<Object> keySet) {
      HashMap<Object, Object> previousValues = new HashMap<>();
      for (Object key : keySet) {
         InternalCacheEntry internalCacheEntry = dataContainer.get(key);
         Object previousValue = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         previousValues.put(key, previousValue);
      }
      return previousValues;
   }

   /**
    * Indexing management of a ReplaceCommand
    *
    * @param command the ReplaceCommand
    * @param ctx the InvocationContext
    * @param valueReplaced the previous value on this key
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processReplaceCommand(final ReplaceCommand command, final InvocationContext ctx, final Object valueReplaced, TransactionContext transactionContext) {
      if (valueReplaced != null && command.isSuccessful() && shouldModifyIndexes(command, ctx)) {
         final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
         Object p2 = extractValue(command.getNewValue());
         final boolean newValueIsIndexed = updateKnownTypesIfNeeded(p2);
         Object key = extractValue(command.getKey());

         if (!usingSkipIndexCleanupFlag) {
            final Object p1 = extractValue(command.getOldValue());
            final boolean originalIsIndexed = updateKnownTypesIfNeeded(p1);
            if (p1 != null && originalIsIndexed) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               removeFromIndexes(p1, key, transactionContext);
            }
         }
         if (newValueIsIndexed) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            updateIndexes(usingSkipIndexCleanupFlag, p2, key, transactionContext);
         }
      }
   }

   /**
    * Indexing management of a RemoveCommand
    *
    * @param command the visited RemoveCommand
    * @param ctx the InvocationContext of the RemoveCommand
    * @param valueRemoved the value before the removal
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processRemoveCommand(final RemoveCommand command, final InvocationContext ctx, final Object valueRemoved, TransactionContext transactionContext) {
      if (command.isSuccessful() && !command.isNonExistent() && shouldModifyIndexes(command, ctx)) {
         final Object value = extractValue(valueRemoved);
         if (updateKnownTypesIfNeeded(value)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(value, extractValue(command.getKey()), transactionContext);
         }
      }
   }

   /**
    * Indexing management of a PutMapCommand
    *
    * @param command the visited PutMapCommand
    * @param ctx the InvocationContext of the PutMapCommand
    * @param previousValues a map with the previous values, before processing the given PutMapCommand
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processPutMapCommand(final PutMapCommand command, final InvocationContext ctx, final Map<Object, Object> previousValues, TransactionContext transactionContext) {
      if (shouldModifyIndexes(command, ctx)) {
         Map<Object, Object> dataMap = command.getMap();
         final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
         // Loop through all the keys and put those key-value pairings into lucene.
         for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
            final Object key = extractValue(entry.getKey());
            final Object value = extractValue(entry.getValue());
            final Object previousValue = previousValues.get(key);
            if (!usingSkipIndexCleanupFlag && updateKnownTypesIfNeeded(previousValue)) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               removeFromIndexes(previousValue, key, transactionContext);
            }
            if (updateKnownTypesIfNeeded(value)) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               updateIndexes(usingSkipIndexCleanupFlag, value, key, transactionContext);
            }
         }
      }
   }

   /**
    * Indexing management of a PutKeyValueCommand
    *
    * @param command the visited PutKeyValueCommand
    * @param ctx the InvocationContext of the PutKeyValueCommand
    * @param previousValue the value being replaced by the put operation
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processPutKeyValueCommand(final PutKeyValueCommand command, final InvocationContext ctx, final Object previousValue, TransactionContext transactionContext) {
      final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
      //whatever the new type, we might still need to cleanup for the previous value (and schedule removal first!)
      Object value = extractValue(command.getValue());
      if (!usingSkipIndexCleanupFlag && updateKnownTypesIfNeeded(previousValue) && shouldRemove(value, previousValue)) {
         if (shouldModifyIndexes(command, ctx)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(previousValue, extractValue(command.getKey()), transactionContext);
         }
      }
      if (updateKnownTypesIfNeeded(value)) {
         if (shouldModifyIndexes(command, ctx)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            updateIndexes(usingSkipIndexCleanupFlag, value, extractValue(command.getKey()), transactionContext);
         }
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

   /**
    * Indexing management of the Clear command
    *
    * @param command the ClearCommand
    * @param ctx the InvocationContext of the PutKeyValueCommand
    * @param transactionContext Optional for lazy initialization, or to reuse an existing transactional context.
    */
   private void processClearCommand(final ClearCommand command, final InvocationContext ctx, TransactionContext transactionContext) {
      if (shouldModifyIndexes(command, ctx)) {
         purgeAllIndexes(transactionContext);
      }
   }

   private TransactionContext makeTransactionalEventContext() {
      return new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry);
   }

   private boolean usingSkipIndexCleanup(final LocalFlagAffectedCommand command) {
      return command != null && command.hasFlag(Flag.SKIP_INDEX_CLEANUP);
   }

   public IndexModificationStrategy getIndexModificationMode() {
      return indexingMode;
   }

   public boolean isStopping() {
      return stopping.get();
   }

}
