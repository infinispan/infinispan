package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.AbstractWriteKeyCommand;
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
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.PersistenceMode;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.function.TriConsumer;
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
   final AtomicLong cacheStores = new AtomicLong(0);
   protected PersistenceManager persistenceManager;
   private InternalEntryFactory entryFactory;
   private TransactionManager transactionManager;
   private StreamingMarshaller marshaller;

   private TriConsumer<InvocationContext, AbstractWriteKeyCommand, Object> afterWriteCommand = this::afterWriteCommand;

   protected Log getLog() {
      return log;
   }

   @Inject
   protected void init(PersistenceManager pm, InternalEntryFactory entryFactory, TransactionManager transactionManager,
                       StreamingMarshaller marshaller) {
      this.persistenceManager = pm;
      this.entryFactory = entryFactory;
      this.transactionManager = transactionManager;
      this.marshaller = marshaller;
   }

   @Start(priority = 15)
   protected void start() {
      this.setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
   }

   @Override
   public InvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      commitCommand(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
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
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
               if (!isStoreEnabled(rCommand) || rCtx.isInTxScope() || !rCommand.isSuccessful()) return;
               if (!isProperWriter(rCtx, rCommand, rCommand.getKey())) return;

               Object key = rCommand.getKey();
               boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
               if (trace)
                  getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
            });
   }

   @Override
   public InvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isStoreEnabled(command) && !ctx.isInTxScope())
         persistenceManager.clearAllStores(ctx.isOriginLocal() ? BOTH : PRIVATE);

      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
               if (!isStoreEnabled(rCommand) || rCtx.isInTxScope() || !rCommand.isSuccessful())
                  return;
               if (!isProperWriter(rCtx, rCommand, rCommand.getKey()))
                  return;

               Object key = rCommand.getKey();
               storeEntry(rCtx, key, rCommand);
               if (getStatisticsEnabled())
                  cacheStores.incrementAndGet();
            });
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
               if (!isStoreEnabled(rCommand) || rCtx.isInTxScope() || !rCommand.isSuccessful())
                  return;
               if (!isProperWriter(rCtx, rCommand, rCommand.getKey()))
                  return;

               Object key = rCommand.getKey();
               storeEntry(rCtx, key, rCommand);
               if (getStatisticsEnabled())
                  cacheStores.incrementAndGet();
            });
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
               if (!isStoreEnabled(rCommand) || rCtx.isInTxScope())
                  return;

               Map<Object, Object> map = rCommand.getMap();
               for (Object key : map.keySet()) {
                  if (isProperWriter(rCtx, rCommand, key)) {
                     storeEntry(rCtx, key, rCommand);
                  }
               }
               if (getStatisticsEnabled())
                  cacheStores.getAndAdd(map.size());
            });
   }

   @Override
   public InvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return visitWriteCommand(ctx, command);
   }


   private InvocationStage visitWriteCommand(InvocationContext ctx, AbstractWriteKeyCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, afterWriteCommand);
   }

   private void afterWriteCommand(InvocationContext rCtx, AbstractWriteKeyCommand rCommand, Object ignored) {
      if (!isStoreEnabled(rCommand) || rCtx.isInTxScope() || !rCommand.isSuccessful())
         return;
      if (!isProperWriter(rCtx, rCommand, rCommand.getKey()))
         return;

      Param<PersistenceMode> persistMode = rCommand.getParams()
                                                   .get(PersistenceMode.ID);
      switch (persistMode.get()) {
         case PERSIST:
            Object key = rCommand.getKey();
            CacheEntry entry = rCtx.lookupEntry(key);
            if (entry != null) {
               if (entry.isRemoved()) {
                  boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
                  if (trace)
                     getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key,
                                     resp);
               } else if (entry.isChanged()) {
                  storeEntry(rCtx, key, rCommand);
               }
            }
            log.trace("Skipping cache store since entry was not found in context");
            break;
         case SKIP:
            log.trace("Skipping cache store since persistence mode parameter is SKIP");
      }
   }

   @Override
   public InvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public InvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command)
         throws Throwable {
      return visitWriteManyCommand(ctx, command);
   }

   private <T extends WriteCommand & FunctionalCommand> InvocationStage visitWriteManyCommand(InvocationContext ctx,
                                                                                              T command) throws Throwable {
      return invokeNext(ctx, command)
            .thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
               if (!isStoreEnabled(rCommand) || rCtx.isInTxScope())
                  return;

               Param<PersistenceMode> persistMode = rCommand.getParams()
                                                            .get(PersistenceMode.ID);
               switch (persistMode.get()) {
                  case PERSIST:
                     int storedCount = 0;
                     for (Object key : ((WriteCommand) rCommand).getAffectedKeys()) {
                        CacheEntry entry = rCtx.lookupEntry(key);
                        if (entry != null) {
                           if (entry.isRemoved()) {
                              boolean resp = persistenceManager.deleteFromAllStores(key, BOTH);
                              if (trace)
                                 getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key,
                                                 resp);
                           } else {
                              if (entry.isChanged() && isProperWriter(rCtx, rCommand, key)) {
                                 storeEntry(rCtx, key, rCommand);
                                 storedCount++;
                              }
                           }
                        }
                     }

                     if (getStatisticsEnabled())
                        cacheStores.getAndAdd(storedCount);
                     break;
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


      Updater modsBuilder = new Updater(getStatisticsEnabled());
      for (WriteCommand cacheCommand : modifications) {
         if (isStoreEnabled(cacheCommand)) {
            cacheCommand.acceptVisitor(ctx, modsBuilder);
         }
      }
      if (getStatisticsEnabled() && modsBuilder.putCount > 0) {
         cacheStores.getAndAdd(modsBuilder.putCount);
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

   public class Updater extends AbstractVisitor {

      protected final boolean generateStatistics;
      int putCount;

      public Updater(boolean generateStatistics) {
         this.generateStatistics = generateStatistics;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return visitSingleStore(ctx, command, command.getKey());
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         if (isProperWriter(ctx, command, command.getKey())) {
            if (generateStatistics) putCount++;
            CacheEntry entry = ctx.lookupEntry(command.getKey());
            InternalCacheEntry ice;
            if (entry instanceof InternalCacheEntry) {
               ice = (InternalCacheEntry) entry;
            } else if (entry instanceof DeltaAwareCacheEntry) {
               AtomicHashMap<?,?> uncommittedChanges = ((DeltaAwareCacheEntry) entry).getUncommittedChages();
               ice = entryFactory.create(entry.getKey(), uncommittedChanges, entry.getMetadata(), entry.getLifespan(), entry.getMaxIdle());
            } else {
               ice = entryFactory.create(entry);
            }
            MarshalledEntryImpl marshalledEntry = new MarshalledEntryImpl(ice.getKey(), ice.getValue(), internalMetadata(ice), marshaller);
            persistenceManager.writeToAllNonTxStores(marshalledEntry, skipSharedStores(ctx, command.getKey(), command) ? PRIVATE : BOTH);
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return visitSingleStore(ctx, command, command.getKey());
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         Map<Object, Object> map = command.getMap();
         for (Object key : map.keySet())
            visitSingleStore(ctx, command, key);
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         Object key = command.getKey();
         if (isProperWriter(ctx, command, key)) {
            persistenceManager.deleteFromAllStores(key, skipSharedStores(ctx, key, command) ? PRIVATE : BOTH);
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         persistenceManager.clearAllStores(ctx.isOriginLocal() ? PRIVATE : BOTH);
         return null;
      }

      protected Object visitSingleStore(InvocationContext ctx, FlagAffectedCommand command, Object key) throws Throwable {
         if (isProperWriter(ctx, command, key)) {
            if (generateStatistics) putCount++;
            InternalCacheValue sv = entryFactory.getValueFromCtxOrCreateNew(key, ctx);
            MarshalledEntryImpl me = new MarshalledEntryImpl(key, sv.getValue(), internalMetadata(sv), marshaller);
            persistenceManager.writeToAllNonTxStores(me, skipSharedStores(ctx, key, command) ? PRIVATE : BOTH);
         }
         return null;
      }
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
      InternalCacheValue sv = getStoredValue(key, ctx);
      persistenceManager.writeToAllNonTxStores(new MarshalledEntryImpl(key, sv.getValue(), internalMetadata(sv), marshaller),
                                               skipSharedStores(ctx, key, command) ? PRIVATE : BOTH);
      if (trace) getLog().tracef("Stored entry %s under key %s", sv, key);
   }

   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !ctx.isOriginLocal() ||
            command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }

   InternalCacheValue getStoredValue(Object key, InvocationContext ctx) {
      return entryFactory.getValueFromCtxOrCreateNew(key, ctx);
   }
}
