package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.functional.FunctionalCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.functional.Param;
import org.infinispan.functional.Param.PersistenceMode;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Writes modifications back to the store on the way out: stores modifications back through the CacheLoader, either
 * after each method call (no TXs), or at TX commit.
 *
 * Only used for LOCAL and INVALIDATION caches.
 *
 * @author Bela Ban
 * @author Dan Berindei
 * @author Mircea Markus
 * @since 9.0
 */
@MBean(objectName = "CacheStore", description = "Component that handles storing of entries to a CacheStore from memory.")
public class CacheWriterInterceptor extends JmxStatsCommandInterceptor {
   private static final Log log = LogFactory.getLog(CacheWriterInterceptor.class);
   private final boolean trace = getLog().isTraceEnabled();

   @Inject protected PersistenceManager persistenceManager;
   @Inject InternalEntryFactory entryFactory;
   @Inject TransactionManager transactionManager;
   @Inject KeyPartitioner keyPartitioner;
   @Inject MarshallableEntryFactory marshalledEntryFactory;

   PersistenceConfiguration loaderConfig = null;
   final AtomicLong cacheStores = new AtomicLong(0);

   protected InvocationSuccessFunction<PutMapCommand> handlePutMapCommandReturn = this::handlePutMapCommandReturn;

   protected Log getLog() {
      return log;
   }


   @Start(priority = 15)
   protected void start() {
      this.setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
      loaderConfig = cacheConfiguration.persistence();
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return asyncInvokeNext(ctx, command, commitCommand(ctx));
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         return asyncInvokeNext(ctx, command, commitCommand(ctx));
      }
      return invokeNext(ctx, command);
   }

   protected CompletionStage<Void> commitCommand(TxInvocationContext ctx) throws Throwable {
      if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         // this is a commit call.
         GlobalTransaction tx = ctx.getGlobalTransaction();
         if (trace) getLog().tracef("Calling loader.commit() for transaction %s", tx);

         Transaction xaTx = null;
         try {
            xaTx = suspendRunningTx(ctx);
            return store(ctx);
         } finally {
            resumeRunningTx(xaTx);
         }
      } else {
         if (trace) getLog().trace("Commit called with no modifications; ignoring.");
         return null;
      }
   }

   private void resumeRunningTx(Transaction xaTx) throws InvalidTransactionException, SystemException {
      if (transactionManager != null && xaTx != null) {
         transactionManager.resume(xaTx);
      }
   }

   private Transaction suspendRunningTx(TxInvocationContext ctx) throws SystemException {
      Transaction xaTx = null;
      if (transactionManager != null) {
         xaTx = transactionManager.suspend();
         if (xaTx != null && !ctx.isOriginLocal())
            throw new IllegalStateException("It is only possible to be in the context of an JRA transaction in the local node.");
      }
      return xaTx;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, removeCommand, rv) -> {
         if (!isStoreEnabled(removeCommand) || rCtx.isInTxScope() || !removeCommand.isSuccessful() ||
               !isProperWriter(rCtx, removeCommand, removeCommand.getKey())) {
            return rv;
         }

         Object key = removeCommand.getKey();
         CompletionStage<?> stage = persistenceManager.deleteFromAllStores(key, removeCommand.getSegment(), BOTH);
         if (trace) {
            stage = stage.thenAccept(removed ->
                  getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
         } else {

         }
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope()) {
         return asyncInvokeNext(ctx, command, persistenceManager.clearAllStores(ctx.isOriginLocal() ? BOTH : PRIVATE));
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, putKeyValueCommand, rv) -> {
         if (!isStoreEnabled(putKeyValueCommand) || rCtx.isInTxScope() || !putKeyValueCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, putKeyValueCommand, putKeyValueCommand.getKey()))
            return rv;

         Object key = putKeyValueCommand.getKey();
         return delayedValue(storeEntry(rCtx, key, putKeyValueCommand), rv);
      });
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, replaceCommand, rv) -> {
         if (!isStoreEnabled(replaceCommand) || rCtx.isInTxScope() || !replaceCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, replaceCommand, replaceCommand.getKey()))
            return rv;

         Object key = replaceCommand.getKey();
         return delayedValue(storeEntry(rCtx, key, replaceCommand), rv);
      });
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, computeCommand, rv) -> {
         if (!isStoreEnabled(computeCommand) || rCtx.isInTxScope() || !computeCommand.isSuccessful() ||
               !isProperWriter(rCtx, computeCommand, computeCommand.getKey()))
            return rv;

         Object key = computeCommand.getKey();
         CompletionStage<?> resultStage;
         if(rv == null) {
            CompletionStage<Boolean> stage = persistenceManager.deleteFromAllStores(key, computeCommand.getSegment(), BOTH);
            if (trace) {
               resultStage = stage.thenAccept(removed ->
                     getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
            } else {
               resultStage = stage;
            }
         } else {
            resultStage = storeEntry(rCtx, key, computeCommand);
         }
         return delayedValue(resultStage, rv);
      });
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, computeIfAbsentCommand, rv) -> {
         if (!isStoreEnabled(computeIfAbsentCommand) || rCtx.isInTxScope() || !computeIfAbsentCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, computeIfAbsentCommand, computeIfAbsentCommand.getKey()))
            return rv;

         if (rv != null) {
            Object key = computeIfAbsentCommand.getKey();
            return delayedValue(storeEntry(rCtx, key, computeIfAbsentCommand), rv);
         }
         return rv;
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handlePutMapCommandReturn);
   }

   protected Object handlePutMapCommandReturn(InvocationContext rCtx, PutMapCommand putMapCommand, Object rv) {
      if (!isStoreEnabled(putMapCommand) || rCtx.isInTxScope())
         return rv;


      CompletionStage<Void> writeStage = CompletionStages.allOf(
            processIterableBatch(rCtx, putMapCommand, BOTH, key -> !skipSharedStores(rCtx, key, putMapCommand)),
            processIterableBatch(rCtx, putMapCommand, PRIVATE, key -> skipSharedStores(rCtx, key, putMapCommand)));
      return delayedValue(writeStage, rv);
   }

   protected CompletionStage<Void> processIterableBatch(InvocationContext ctx, PutMapCommand cmd, PersistenceManager.AccessMode mode, Predicate<Object> filter) {
      if (getStatisticsEnabled())
         cacheStores.addAndGet(cmd.getMap().size());

      Iterable<MarshallableEntry> iterable = () -> cmd.getMap().keySet().stream()
            .filter(filter)
            .map(key -> marshalledEntry(ctx, key))
            .filter(StreamMarshalling.nonNullPredicate())
            .iterator();
      return persistenceManager.writeBatchToAllNonTxStores(iterable, mode, cmd.getFlagsBitSet());
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   private <T extends DataWriteCommand & FunctionalCommand> Object visitWriteCommand(InvocationContext ctx, T command) {
      return invokeNextThenApply(ctx, command, (rCtx, dataWriteCommand, rv) -> {
         if (!isStoreEnabled(dataWriteCommand) || rCtx.isInTxScope() || !dataWriteCommand.isSuccessful() ||
               !isProperWriter(rCtx, dataWriteCommand, dataWriteCommand.getKey()))
            return rv;

         CompletionStage<?> stage = CompletableFutures.completedNull();
         Param<PersistenceMode> persistMode = dataWriteCommand.getParams().get(PersistenceMode.ID);
         switch (persistMode.get()) {
            case LOAD_PERSIST:
            case SKIP_LOAD:
               Object key = dataWriteCommand.getKey();
               CacheEntry entry = rCtx.lookupEntry(key);
               if (entry != null) {
                  if (entry.isRemoved()) {
                     stage = persistenceManager.deleteFromAllStores(key, dataWriteCommand.getSegment(), BOTH);
                     if (trace) {
                        stage = stage.thenAccept(removed ->
                              getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
                     }
                  } else if (entry.isChanged()) {
                     stage = storeEntry(rCtx, key, dataWriteCommand);
                     if (trace) {
                        stage = stage.thenAccept(removed ->
                              getLog().tracef("Stored entry for key %s in CacheStore", key));
                     }
                  } else if (trace) {
                     getLog().tracef("Skipping write for key %s as entry wasn't changed");
                  }
               }
               log.trace("Skipping cache store since entry was not found in context");
               break;
            case SKIP_PERSIST:
            case SKIP:
               log.trace("Skipping cache store since persistence mode parameter is SKIP");
               break;
         }
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   private <T extends WriteCommand & FunctionalCommand> Object visitWriteManyCommand(InvocationContext ctx,
                                                                                                   T command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, manyEntriesCommand, rv) -> {
         if (!isStoreEnabled(manyEntriesCommand) || rCtx.isInTxScope())
            return rv;

         CompletionStage<Void> stage = CompletableFutures.completedNull();
         Param<PersistenceMode> persistMode = manyEntriesCommand.getParams().get(PersistenceMode.ID);
         switch (persistMode.get()) {
            case LOAD_PERSIST:
            case SKIP_LOAD:
               AggregateCompletionStage<Void> composedCompletionStage = CompletionStages.aggregateCompletionStage();
               int storedCount = 0;
               for (Object key : manyEntriesCommand.getAffectedKeys()) {
                  CacheEntry entry = rCtx.lookupEntry(key);
                  if (entry != null) {
                     if (entry.isRemoved()) {
                        CompletionStage<?> innerStage = persistenceManager.deleteFromAllStores(key,
                              keyPartitioner.getSegment(key), BOTH);
                        if (trace) {
                           innerStage = innerStage.thenAccept(removed ->
                                 getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
                        }
                        composedCompletionStage.dependsOn(innerStage);
                     } else {
                        if (entry.isChanged() && isProperWriter(rCtx, manyEntriesCommand, key)) {
                           composedCompletionStage.dependsOn(storeEntry(rCtx, key, manyEntriesCommand, false));
                           storedCount++;
                        }
                     }
                  }
               }

               if (getStatisticsEnabled())
                  cacheStores.getAndAdd(storedCount);
               stage = composedCompletionStage.freeze();
               break;
            case SKIP_PERSIST:
            case SKIP:
               log.trace("Skipping cache store since persistence mode parameter is SKIP");
               break;
         }
         return delayedValue(stage, rv);
      });
   }

   protected final CompletionStage<Void> store(TxInvocationContext ctx) throws Throwable {
      List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
      if (modifications.isEmpty()) {
         if (trace) getLog().trace("Transaction has not logged any modifications!");
         return CompletableFutures.completedNull();
      }
      if (trace) getLog().tracef("Cache loader modification list: %s", modifications);


      TxBatchUpdater modsBuilder = TxBatchUpdater.createNonTxStoreUpdater(this, persistenceManager, entryFactory, marshalledEntryFactory);
      for (WriteCommand cacheCommand : modifications) {
         if (isStoreEnabled(cacheCommand)) {
            cacheCommand.acceptVisitor(ctx, modsBuilder);
         }
      }
      BatchModification sharedMods = modsBuilder.getModifications();
      BatchModification nonSharedMods = modsBuilder.getNonSharedModifications();

      // Performs all the batch writes in parallel
      CompletionStage<Void> writeStage = CompletionStages.allOf(
            persistenceManager.writeBatchToAllNonTxStores(sharedMods.getMarshallableEntries(), BOTH, 0),
            persistenceManager.writeBatchToAllNonTxStores(nonSharedMods.getMarshallableEntries(), PRIVATE, 0),
            persistenceManager.deleteBatchFromAllNonTxStores(sharedMods.getKeysToRemove(), BOTH, 0),
            persistenceManager.deleteBatchFromAllNonTxStores(nonSharedMods.getKeysToRemove(), PRIVATE, 0));

      if (trace) {
         getLog().tracef("Writing shared batch with #entries=%d and non-shared batch with #entries=%d", sharedMods.getMarshallableEntries().size(), nonSharedMods.getMarshallableEntries().size());
         getLog().tracef("Deleting shared batch with #entries=%d and non-shared batch with #entries=%d", sharedMods.getKeysToRemove().size(), nonSharedMods.getKeysToRemove().size());
      }

      if (getStatisticsEnabled() && modsBuilder.getPutCount() > 0) {
         cacheStores.getAndAdd(modsBuilder.getPutCount());
      }
      return writeStage;
   }

   protected boolean isStoreEnabled(FlagAffectedCommand command) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_CACHE_STORE)) {
         log.trace("Skipping cache store since the call contain a skip cache store flag");
         return false;
      }
      return true;
   }

   protected boolean isProperWriter(InvocationContext ctx, FlagAffectedCommand command, Object key) {
      return true;
   }

   @Override
   public void resetStatistics() {
      cacheStores.set(0);
   }

   @ManagedAttribute(
         description = "Number of writes to the store",
         displayName = "Number of writes to the store",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getWritesToTheStores() {
      return cacheStores.get();
   }

   @ManagedAttribute(
         description = "Number of entries currently persisted excluding expired entries",
         displayName = "Number of persisted entries"
   )
   public int getNumberOfPersistedEntries() {
      return CompletionStages.join(persistenceManager.size());
   }

   CompletionStage<Void> storeEntry(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return storeEntry(ctx, key, command, true);
   }

   CompletionStage<Void> storeEntry(InvocationContext ctx, Object key, FlagAffectedCommand command, boolean incrementStats) {
      if (persistenceManager.isReadOnly())
         return CompletableFutures.completedNull();

      MarshallableEntry entry = marshalledEntry(ctx, key);
      if (entry != null) {
         CompletionStage<Void> stage = persistenceManager.writeToAllNonTxStores(entry,
               SegmentSpecificCommand.extractSegment(command, key, keyPartitioner),
               skipSharedStores(ctx, key, command) ? PRIVATE : BOTH, command.getFlagsBitSet());
         if (trace) {
            stage = stage.thenAccept(ignore ->
               getLog().tracef("Stored entry %s under key %s", entry.getValue(), key));
         }
         if (incrementStats && getStatisticsEnabled()) {
            stage = stage.thenAccept(ignore ->
                  cacheStores.incrementAndGet());
         }
         return stage;
      }
      return CompletableFutures.completedNull();
   }

   MarshallableEntry marshalledEntry(InvocationContext ctx, Object key) {
      InternalCacheValue sv = entryFactory.getValueFromCtx(key, ctx);
      return sv != null ? marshalledEntryFactory.create(key, sv.getValue(), sv.getMetadata(), sv.getCreated(), sv.getLastUsed()) : null;
   }

   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !ctx.isOriginLocal() ||
            command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }
}
