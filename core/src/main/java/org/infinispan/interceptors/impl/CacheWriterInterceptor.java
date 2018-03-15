package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
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
import org.infinispan.functional.Param;
import org.infinispan.functional.Param.PersistenceMode;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.transaction.xa.GlobalTransaction;
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
   @Inject private InternalEntryFactory entryFactory;
   @Inject private TransactionManager transactionManager;
   @Inject private StreamingMarshaller marshaller;

   PersistenceConfiguration loaderConfig = null;
   final AtomicLong cacheStores = new AtomicLong(0);

   protected InvocationSuccessAction handlePutMapCommandReturn = this::handlePutMapCommandReturn;

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
      commitCommand(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         commitCommand(ctx);
      }
      return invokeNext(ctx, command);
   }

   protected void commitCommand(TxInvocationContext ctx) throws Throwable {
      if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         // this is a commit call.
         GlobalTransaction tx = ctx.getGlobalTransaction();
         if (trace) getLog().tracef("Calling loader.commit() for transaction %s", tx);

         Transaction xaTx = null;
         try {
            xaTx = suspendRunningTx(ctx);
            store(ctx);
         } finally {
            resumeRunningTx(xaTx);
         }
      } else {
         if (trace) getLog().trace("Commit called with no modifications; ignoring.");
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
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         RemoveCommand removeCommand = (RemoveCommand) rCommand;
         if (!isStoreEnabled(removeCommand) || rCtx.isInTxScope() || !removeCommand.isSuccessful()) return;
         if (!isProperWriter(rCtx, removeCommand, removeCommand.getKey())) return;

         Object key = removeCommand.getKey();
         boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
         if (trace)
            getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      });
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope())
         persistenceManager.clearAllStores(ctx.isOriginLocal() ? BOTH : PRIVATE);

      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         PutKeyValueCommand putKeyValueCommand = (PutKeyValueCommand) rCommand;
         if (!isStoreEnabled(putKeyValueCommand) || rCtx.isInTxScope() || !putKeyValueCommand.isSuccessful())
            return;
         if (!isProperWriter(rCtx, putKeyValueCommand, putKeyValueCommand.getKey()))
            return;

         Object key = putKeyValueCommand.getKey();
         storeEntry(rCtx, key, putKeyValueCommand);
         if (getStatisticsEnabled())
            cacheStores.incrementAndGet();
      });
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         ReplaceCommand replaceCommand = (ReplaceCommand) rCommand;
         if (!isStoreEnabled(replaceCommand) || rCtx.isInTxScope() || !replaceCommand.isSuccessful())
            return;
         if (!isProperWriter(rCtx, replaceCommand, replaceCommand.getKey()))
            return;

         Object key = replaceCommand.getKey();
         storeEntry(rCtx, key, replaceCommand);
         if (getStatisticsEnabled())
            cacheStores.incrementAndGet();
      });
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         ComputeCommand computeCommand = (ComputeCommand) rCommand;
         if (!isStoreEnabled(computeCommand) || rCtx.isInTxScope() || !computeCommand.isSuccessful())
            return;
         if (!isProperWriter(rCtx, computeCommand, computeCommand.getKey()))
            return;

         Object key = computeCommand.getKey();
         if(rv == null) {
            boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
            if (trace)
               getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
         } else {
            storeEntry(rCtx, key, computeCommand);
            if (getStatisticsEnabled())
               cacheStores.incrementAndGet();
         }
      });
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         ComputeIfAbsentCommand computeIfAbsentCommand = (ComputeIfAbsentCommand) rCommand;
         if (!isStoreEnabled(computeIfAbsentCommand) || rCtx.isInTxScope() || !computeIfAbsentCommand.isSuccessful())
            return;
         if (!isProperWriter(rCtx, computeIfAbsentCommand, computeIfAbsentCommand.getKey()))
            return;

         if (rv != null) {
            Object key = computeIfAbsentCommand.getKey();
            storeEntry(rCtx, key, computeIfAbsentCommand);
            if (getStatisticsEnabled())
               cacheStores.incrementAndGet();
         }
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, handlePutMapCommandReturn);
   }

   protected void handlePutMapCommandReturn(InvocationContext rCtx, VisitableCommand rCommand, Object rv) {
      PutMapCommand putMapCommand = (PutMapCommand) rCommand;
      if (!isStoreEnabled(putMapCommand) || rCtx.isInTxScope())
         return;

      processIterableBatch(rCtx, putMapCommand, BOTH, key -> !skipSharedStores(rCtx, key, putMapCommand));
      processIterableBatch(rCtx, putMapCommand, PRIVATE, key -> skipSharedStores(rCtx, key, putMapCommand));
   }

   protected void processIterableBatch(InvocationContext ctx, PutMapCommand cmd, PersistenceManager.AccessMode mode, Predicate<Object> filter) {
      if (getStatisticsEnabled())
         cacheStores.addAndGet(cmd.getMap().size());

      Iterable<MarshalledEntry> iterable = () -> cmd.getMap().keySet().stream()
            .filter(filter)
            .map(key -> createMarshalledEntry(ctx, key))
            .iterator();
      persistenceManager.writeBatchToAllNonTxStores(iterable, mode, cmd.getFlagsBitSet());
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

   private <T extends DataWriteCommand & FunctionalCommand> Object visitWriteCommand(InvocationContext ctx,
         VisitableCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         T dataWriteCommand = (T) rCommand;
         if (!isStoreEnabled(dataWriteCommand) || rCtx.isInTxScope() || !dataWriteCommand.isSuccessful())
            return;
         if (!isProperWriter(rCtx, dataWriteCommand, dataWriteCommand.getKey()))
            return;

         Param<PersistenceMode> persistMode = dataWriteCommand.getParams().get(PersistenceMode.ID);
         switch (persistMode.get()) {
            case LOAD_PERSIST:
            case SKIP_LOAD:
               Object key = dataWriteCommand.getKey();
               CacheEntry entry = rCtx.lookupEntry(key);
               if (entry != null) {
                  if (entry.isRemoved()) {
                     boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
                     if (trace)
                        getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key,
                              resp);
                  } else if (entry.isChanged()) {
                     storeEntry(rCtx, key, dataWriteCommand);
                  }
               }
               log.trace("Skipping cache store since entry was not found in context");
               break;
            case SKIP_PERSIST:
            case SKIP:
               log.trace("Skipping cache store since persistence mode parameter is SKIP");
         }
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
                                                                                                   WriteCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         T manyEntriesCommand = (T) rCommand;
         if (!isStoreEnabled(manyEntriesCommand) || rCtx.isInTxScope())
            return;

         Param<PersistenceMode> persistMode = manyEntriesCommand.getParams().get(PersistenceMode.ID);
         switch (persistMode.get()) {
            case LOAD_PERSIST:
            case SKIP_LOAD:
               int storedCount = 0;
               for (Object key : ((WriteCommand) rCommand).getAffectedKeys()) {
                  CacheEntry entry = rCtx.lookupEntry(key);
                  if (entry != null) {
                     if (entry.isRemoved()) {
                        boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
                        if (trace) getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
                     } else {
                        if (entry.isChanged() && isProperWriter(rCtx, manyEntriesCommand, key)) {
                           storeEntry(rCtx, key, manyEntriesCommand);
                           storedCount++;
                        }
                     }
                  }
               }

               if (getStatisticsEnabled())
                  cacheStores.getAndAdd(storedCount);
               break;
            case SKIP_PERSIST:
            case SKIP:
               log.trace("Skipping cache store since persistence mode parameter is SKIP");
         }
      });
   }

   protected final void store(TxInvocationContext ctx) throws Throwable {
      List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
      if (modifications.isEmpty()) {
         if (trace) getLog().trace("Transaction has not logged any modifications!");
         return;
      }
      if (trace) getLog().tracef("Cache loader modification list: %s", modifications);


      TxBatchUpdater modsBuilder = TxBatchUpdater.createNonTxStoreUpdater(this, persistenceManager, entryFactory, marshaller);
      for (WriteCommand cacheCommand : modifications) {
         if (isStoreEnabled(cacheCommand)) {
            cacheCommand.acceptVisitor(ctx, modsBuilder);
         }
      }
      BatchModification sharedMods = modsBuilder.getModifications();
      BatchModification nonSharedMods = modsBuilder.getNonSharedModifications();

      persistenceManager.writeBatchToAllNonTxStores(sharedMods.getMarshalledEntries(), BOTH, 0);
      persistenceManager.writeBatchToAllNonTxStores(nonSharedMods.getMarshalledEntries(), PRIVATE, 0);
      persistenceManager.deleteBatchFromAllNonTxStores(sharedMods.getKeysToRemove(), BOTH, 0);
      persistenceManager.deleteBatchFromAllNonTxStores(nonSharedMods.getKeysToRemove(), PRIVATE, 0);

      if (trace) {
         getLog().tracef("Writing shared batch with #entries=%d and non-shared batch with #entries=%d", sharedMods.getMarshalledEntries().size(), nonSharedMods.getMarshalledEntries().size());
         getLog().tracef("Deleting shared batch with #entries=%d and non-shared batch with #entries=%d", sharedMods.getKeysToRemove().size(), nonSharedMods.getKeysToRemove().size());
      }

      if (getStatisticsEnabled() && modsBuilder.getPutCount() > 0) {
         cacheStores.getAndAdd(modsBuilder.getPutCount());
      }
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
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics"
   )
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

   void storeEntry(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      MarshalledEntry entry = createMarshalledEntry(ctx, key);
      persistenceManager.writeToAllNonTxStores(entry, skipSharedStores(ctx, key, command) ? PRIVATE : BOTH, command.getFlagsBitSet());
      if (trace) getLog().tracef("Stored entry %s under key %s", entry.getValue(), key);
   }

   MarshalledEntry createMarshalledEntry(InvocationContext ctx, Object key) {
      InternalCacheValue sv = entryFactory.getValueFromCtxOrCreateNew(key, ctx);
      return new MarshalledEntryImpl(key, sv.getValue(), internalMetadata(sv), marshaller);
   }

   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !ctx.isOriginLocal() ||
            command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }
}
