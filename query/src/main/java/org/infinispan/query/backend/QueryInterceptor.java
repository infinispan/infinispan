package org.infinispan.query.backend;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.functions.MergeFunction;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
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
import org.infinispan.query.Transformer;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.query.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.LogFactory;

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

   private SearchWorkCreator searchWorkCreator = new DefaultSearchWorkCreator();

   private SearchFactoryHandler searchFactoryHandler;

   private DataContainer dataContainer;
   private DataConversion valueDataConversion;
   private DataConversion keyDataConversion;
   protected TransactionManager transactionManager;
   protected TransactionSynchronizationRegistry transactionSynchronizationRegistry;
   private DistributionManager distributionManager;
   private RpcManager rpcManager;
   protected ExecutorService asyncExecutor;
   protected InternalCacheRegistry internalCacheRegistry;

   private static final Log log = LogFactory.getLog(QueryInterceptor.class, Log.class);

   /**
    * The classes declared by the indexing config as indexable. In 8.2 this can be null, indicating that no classes
    * were declared and we are running in the (deprecated) autodetect mode. Autodetect mode will be removed in 9.0.
    */
   private Class<?>[] indexedEntities;
   private final Cache cache;

   public QueryInterceptor(SearchIntegrator searchFactory, IndexModificationStrategy indexingMode, Cache cache) {
      this.searchFactory = searchFactory;
      this.indexingMode = indexingMode;
      this.cache = cache;
      this.valueDataConversion = cache.getAdvancedCache().getValueDataConversion();
      this.keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();
   }

   public void setValueDataConversion(DataConversion dataConversion) {
      this.valueDataConversion = dataConversion;
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(TransactionManager transactionManager,
                                     TransactionSynchronizationRegistry transactionSynchronizationRegistry,
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
      this.internalCacheRegistry = internalCacheRegistry;
   }

   @Start
   protected void start() {
      Set<Class<?>> indexedEntities = cache.getCacheConfiguration().indexing().indexedEntities();
      this.indexedEntities = indexedEntities.isEmpty() ? null : indexedEntities.toArray(new Class<?>[indexedEntities.size()]);
      queryKnownClasses = indexedEntities.isEmpty() ? new QueryKnownClasses(cache.getName(), cache.getCacheManager(), internalCacheRegistry) : new QueryKnownClasses(indexedEntities);
      searchFactoryHandler = new SearchFactoryHandler(searchFactory, queryKnownClasses, new TransactionHelper(transactionManager));
      if (this.indexedEntities == null) {
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

   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx, Object key) {
      return indexingMode.shouldModifyIndexes(command, ctx, distributionManager, rpcManager, key);
   }

   /**
    * Use this executor for Async operations
    */
   public ExecutorService getAsyncExecutor() {
      return asyncExecutor;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processPutKeyValueCommand(((PutKeyValueCommand) rCommand), rCtx, rv, null));
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      // remove the object out of the cache first.
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processRemoveCommand(((RemoveCommand) rCommand), rCtx, rv, null));
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processReplaceCommand(((ReplaceCommand) rCommand), rCtx, rv, null));
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      InternalCacheEntry internalCacheEntry = dataContainer.get(command.getKey());
      Object stateBeforeCompute = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processComputeCommand(((ComputeCommand) rCommand), rCtx, stateBeforeCompute, rv, null));
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      InternalCacheEntry internalCacheEntry = dataContainer.get(command.getKey());
      Object stateBeforeCompute = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processComputeIfAbsentCommand(((ComputeIfAbsentCommand) rCommand), rCtx, stateBeforeCompute, rv, null));
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      command.setFlagsBitSet(EnumUtil.diffBitSets(command.getFlagsBitSet(), FlagBitSets.IGNORE_RETURN_VALUES));
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         Map<Object, Object> previousValues = (Map<Object, Object>) rv;
         processPutMapCommand(((PutMapCommand) rCommand), rCtx, previousValues, null);
      });
   }

   @Override
   public Object visitClearCommand(final InvocationContext ctx, final ClearCommand command)
         throws Throwable {
      // This method is called when somebody calls a cache.clear() and we will need to wipe everything in the indexes.
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processClearCommand(((ClearCommand) rCommand), rCtx, null));
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      InternalCacheEntry internalCacheEntry = dataContainer.get(command.getKey());
      Object stateBefore = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> processReadWriteKeyCommand(((ReadWriteKeyCommand) rCommand), rCtx, stateBefore, rv, null));
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

   /**
    * Remove entries from all indexes by key
    */
   void removeFromIndexes(TransactionContext transactionContext, Object key) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
      Stream<IndexedTypeIdentifier> typeIdentifiers = getKnownClasses().stream().map(PojoIndexedTypeIdentifier::new);
      Set<Work> deleteWorks = typeIdentifiers
            .map(e -> searchWorkCreator.createEntityWork(keyToString(key), e, WorkType.DELETE))
            .collect(Collectors.toSet());
      performSearchWorks(deleteWorks, transactionContext);
   }

   private void purgeIndex(TransactionContext transactionContext, Class<?> entityType) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
      Boolean isIndexable = queryKnownClasses.get(entityType);
      if (isIndexable != null && isIndexable.booleanValue()) {
         if (searchFactoryHandler.hasIndex(entityType)) {
            IndexedTypeIdentifier type = new PojoIndexedTypeIdentifier(entityType);
            performSearchWorks(searchWorkCreator.createPerEntityTypeWorks(type, WorkType.PURGE_ALL), transactionContext);
         }
      }
   }

   private void purgeAllIndexes(TransactionContext transactionContext) {
      transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
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

   private Object extractKey(Object storedValue) {
      return keyDataConversion.extractIndexable(storedValue);
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
      return keyTransformationHandler.keyToString(extractValue(key));
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

   /**
    * In case of a remotely originating transactions we don't have a chance to visit the single
    * commands but receive this "batch". We then need the before-apply snapshot of some types
    * to route the cleanup commands to the correct indexes.
    * Note we don't need to visit the CommitCommand as the indexing context is registered
    * as a transaction sync.
    */
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
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
         } else if (writeCommand instanceof ComputeCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((ComputeCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         } else if (writeCommand instanceof ComputeIfAbsentCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((ComputeIfAbsentCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         } else if (writeCommand instanceof ReadWriteKeyCommand) {
            InternalCacheEntry internalCacheEntry = dataContainer.get(((ReadWriteKeyCommand) writeCommand).getKey());
            stateBeforePrepare[i] = internalCacheEntry != null ? internalCacheEntry.getValue() : null;
         }
      }

      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
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
               } else if (writeCommand instanceof ComputeCommand) {
                  processComputeCommand((ComputeCommand) writeCommand, txInvocationContext, stateBeforePrepare[i], transactionContext);
               } else if (writeCommand instanceof ComputeIfAbsentCommand) {
                  processComputeIfAbsentCommand((ComputeIfAbsentCommand) writeCommand, txInvocationContext, stateBeforePrepare[i], transactionContext);
               } else if (writeCommand instanceof ClearCommand) {
                  processClearCommand((ClearCommand) writeCommand, txInvocationContext, transactionContext);
               } else if (writeCommand instanceof ReadWriteKeyCommand) {
                  ReadWriteKeyCommand readWriteKeyCommand = (ReadWriteKeyCommand) writeCommand;
                  Object resultValue = ctx.lookupEntry(readWriteKeyCommand.getKey()).getValue();
                  processReadWriteKeyCommand((ReadWriteKeyCommand) writeCommand, txInvocationContext, stateBeforePrepare[i], resultValue, transactionContext);
               }
            }
         }
      });
   }

   private Map<Object, Object> getPreviousValues(Set<Object> keySet) {
      Map<Object, Object> previousValues = new HashMap<>();
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
    * @param command            the ReplaceCommand
    * @param ctx                the InvocationContext
    * @param valueReplaced      the previous value on this key
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processReplaceCommand(final ReplaceCommand command, final InvocationContext ctx, final Object valueReplaced, TransactionContext transactionContext) {
      if (valueReplaced != null && command.isSuccessful()) {
         Object key = extractKey(command.getKey());
         if (shouldModifyIndexes(command, ctx, key)) {
            final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
            Object p2 = extractValue(command.getNewValue());
            final boolean newValueIsIndexed = updateKnownTypesIfNeeded(p2);

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
   }

   /**
    * Indexing management of a ComputeCommand
    *
    * @param command the ComputeCommand
    * @param ctx the InvocationContext
    * @param prevValue the previous value on this key
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processComputeCommand(final ComputeCommand command, final InvocationContext ctx, final Object prevValue, TransactionContext transactionContext) {
      if (command.isSuccessful()) {
         processFunctionalCommand(command, ctx, prevValue, ctx.lookupEntry(command.getKey()).getValue(), transactionContext);
      }
   }

   /**
    * Indexing management of a ComputeCommand
    *
    * @param command the ComputeCommand
    * @param ctx the InvocationContext
    * @param prevValue the previous value on this key
    * @param computedValue the computedValue
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processComputeCommand(final ComputeCommand command, final InvocationContext ctx, final Object prevValue, final Object computedValue, TransactionContext transactionContext) {
      if (command.isSuccessful()) {
         processFunctionalCommand(command, ctx, prevValue, computedValue, transactionContext);
      }
   }

   /**
    * Indexing management of a ComputeIfAbsentCommand
    *
    * @param command the ComputeIfAbsentCommand
    * @param ctx the InvocationContext
    * @param prevValue the value before the call
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processComputeIfAbsentCommand(final ComputeIfAbsentCommand command, final InvocationContext ctx, final Object prevValue, TransactionContext transactionContext) {
      if (command.isSuccessful()) {
         processFunctionalCommand(command, ctx, prevValue, ctx.lookupEntry(command.getKey()).getValue(), transactionContext);
      }
   }

   /**
    * Indexing management of a ComputeIfAbsentCommand
    *
    * @param command the ComputeIfAbsentCommand
    * @param ctx the InvocationContext
    * @param prevValue the value before the call
    * @param computedValue the computedValue
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processComputeIfAbsentCommand(final ComputeIfAbsentCommand command, final InvocationContext ctx, final Object prevValue, final Object computedValue, TransactionContext transactionContext) {
      if (command.isSuccessful()) {
         processFunctionalCommand(command, ctx, prevValue, computedValue, transactionContext);
      }
   }

   /**
    * Indexing management of a ReadWriteKeyCommand
    *
    * @param command the ComputeIfAbsentCommand
    * @param ctx the InvocationContext
    * @param prevValue the value before the call
    * @param commandResult the result of the command call
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processReadWriteKeyCommand(final ReadWriteKeyCommand command, final InvocationContext ctx, final Object prevValue, final Object commandResult, TransactionContext transactionContext) {
      if (command.isSuccessful()) {
         if (command.getFunction() instanceof MergeFunction) {
            processFunctionalCommand(command, ctx, prevValue, commandResult, transactionContext);
         } else {
            throw new UnsupportedOperationException("ReadWriteKeyCommand is not supported for query");
         }
      }
   }

   private void processFunctionalCommand(AbstractDataWriteCommand command, InvocationContext ctx, Object prevValue, Object computedValue, TransactionContext transactionContext) {
      Object key = extractKey(command.getKey());
      if (shouldModifyIndexes(command, ctx, key)) {
         final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
         Object p2 = extractValue(computedValue);
         final boolean newValueIsIndexed = updateKnownTypesIfNeeded(p2);

         if (!usingSkipIndexCleanupFlag && updateKnownTypesIfNeeded(prevValue) && shouldRemove(p2, prevValue)) {
            if (shouldModifyIndexes(command, ctx, key)) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               removeFromIndexes(prevValue, extractValue(key), transactionContext);
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
    * @param command            the visited RemoveCommand
    * @param ctx                the InvocationContext of the RemoveCommand
    * @param valueRemoved       the value before the removal
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processRemoveCommand(final RemoveCommand command, final InvocationContext ctx, final Object valueRemoved, TransactionContext transactionContext) {
      if (command.isSuccessful() && !command.isNonExistent()) {
         Object key = extractKey(command.getKey());
         if (shouldModifyIndexes(command, ctx, key)) {
            final Object value = extractValue(command.isConditional() ? command.getValue() : valueRemoved);
            if (updateKnownTypesIfNeeded(value)) {
               transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
               removeFromIndexes(value, key, transactionContext);
            }
         }
      }
   }

   /**
    * Indexing management of a PutMapCommand
    *
    * @param command            the visited PutMapCommand
    * @param ctx                the InvocationContext of the PutMapCommand
    * @param previousValues     a map with the previous values, before processing the given PutMapCommand
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processPutMapCommand(final PutMapCommand command, final InvocationContext ctx, final Map<Object, Object> previousValues, TransactionContext transactionContext) {
      Map<Object, Object> dataMap = command.getMap();
      final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
      // Loop through all the keys and put those key-value pairings into lucene.
      for (Map.Entry<Object, Object> entry : previousValues.entrySet()) {
         Object originalKey = entry.getKey();
         final Object key = extractValue(originalKey);
         final Object value = extractValue(dataMap.get(originalKey));
         final Object previousValue = extractValue(entry.getValue());
         if (!usingSkipIndexCleanupFlag && updateKnownTypesIfNeeded(previousValue)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            if (shouldModifyIndexes(command, ctx, key)) {
               removeFromIndexes(previousValue, key, transactionContext);
            }
         }
         if (updateKnownTypesIfNeeded(value)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            if (shouldModifyIndexes(command, ctx, key)) {
               updateIndexes(usingSkipIndexCleanupFlag, value, key, transactionContext);
            }
         }
      }
   }

   /**
    * Indexing management of a PutKeyValueCommand
    *
    * @param command            the visited PutKeyValueCommand
    * @param ctx                the InvocationContext of the PutKeyValueCommand
    * @param previousValue      the value being replaced by the put operation
    * @param transactionContext Optional for lazy initialization, or reuse an existing context.
    */
   private void processPutKeyValueCommand(final PutKeyValueCommand command, final InvocationContext ctx, final Object previousValue, TransactionContext transactionContext) {
      final boolean usingSkipIndexCleanupFlag = usingSkipIndexCleanup(command);
      //whatever the new type, we might still need to cleanup for the previous value (and schedule removal first!)
      Object value = extractValue(command.getValue());
      Object key = extractKey(command.getKey());
      if (!usingSkipIndexCleanupFlag && updateKnownTypesIfNeeded(previousValue) && shouldRemove(value, previousValue)) {
         if (shouldModifyIndexes(command, ctx, key)) {
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            removeFromIndexes(previousValue, extractValue(key), transactionContext);
         }
      }
      if (updateKnownTypesIfNeeded(value)) {
         if (shouldModifyIndexes(command, ctx, key)) {
            // This means that the entry is just modified so we need to update the indexes and not add to them.
            transactionContext = transactionContext == null ? makeTransactionalEventContext() : transactionContext;
            updateIndexes(usingSkipIndexCleanupFlag, value, extractValue(key), transactionContext);
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
    * @param command            the ClearCommand
    * @param ctx                the InvocationContext of the PutKeyValueCommand
    * @param transactionContext Optional for lazy initialization, or to reuse an existing transactional context.
    */
   private void processClearCommand(final ClearCommand command, final InvocationContext ctx, TransactionContext transactionContext) {
      if (shouldModifyIndexes(command, ctx, null)) {
         purgeAllIndexes(transactionContext);
      }
   }

   private TransactionContext makeTransactionalEventContext() {
      return new TransactionalEventTransactionContext(transactionManager, transactionSynchronizationRegistry);
   }

   private boolean usingSkipIndexCleanup(final FlagAffectedCommand command) {
      return command != null && command.hasAnyFlag(FlagBitSets.SKIP_INDEX_CLEANUP);
   }

   public IndexModificationStrategy getIndexModificationMode() {
      return indexingMode;
   }

   public boolean isStopping() {
      return stopping.get();
   }

}
