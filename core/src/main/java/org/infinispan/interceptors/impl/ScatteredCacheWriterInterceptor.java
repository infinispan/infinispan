package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link DistCacheWriterInterceptor} but as commands are not forwarded from primary owner
 * so we just write to the store all the time (with non-shared interceptors).
 *
 * Uses its own locking scheme to order writes into the store - if there is a newer write into DC,
 * the older write is ignored. This could improve contented case, but has one strange property:
 * Assume CS contains V1, T2 writes V2 and T3 writes V3; if T2 commits before T3 but arrives here after T3,
 * the V2 write into CS is ignored but the operation is confirmed as if it was persisted correctly.
 * If T3 then (spuriously) fails to write into the store, V1 stays in CS but the error is reported
 * only to T3 - that means that T3 effectivelly rolled back V2 into V1 (and reported this as an error).
 * This may surprise some users.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredCacheWriterInterceptor extends CacheWriterInterceptor {

   private static final Log log = LogFactory.getLog(DistCacheWriterInterceptor.class);
   private ClusteringDependentLogic cdl;
   private DataContainer dataContainer;
   private ConcurrentHashMap<Object, CompletableFuture<Void>> locks = new ConcurrentHashMap<>();
   private TimeService timeService;
   private ScheduledExecutorService timeoutExecutor;
   private long lockTimeout;

   private final ReturnHandler dataWriteReturnHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable != null)
         throw throwable;

      DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
      Object key = dataWriteCommand.getKey();
      if (!isStoreEnabled(dataWriteCommand) || !dataWriteCommand.isSuccessful())
         return null;

      CacheEntry cacheEntry = rCtx.lookupEntry(key);
      if (cacheEntry == null) {
         throw new IllegalStateException();
      }
      Metadata metadata = cacheEntry.getMetadata();
      EntryVersion version = metadata == null ? null : metadata.version();
      // version is null only with some nasty flags, we don't care about ordering then
      if (version != null) {
         long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
         return checkLockAndStore(rCtx, dataWriteCommand, key, version, deadline);
      }
      storeEntry(rCtx, key, dataWriteCommand);
      if (getStatisticsEnabled())
         cacheStores.incrementAndGet();
      return null;
   };

   private final ReturnHandler manyDataReturnHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable != null)
         throw throwable;

      FlagAffectedCommand command = (FlagAffectedCommand) rCommand;
      if (!isStoreEnabled(command))
         return null;

      long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
      List<CompletableFuture<Object>> futures = null;
      Map<Object, CacheEntry> lookedUpEntries = rCtx.getLookedUpEntries();
      for (CacheEntry cacheEntry : lookedUpEntries.values()) {
         Metadata metadata = cacheEntry.getMetadata();
         EntryVersion version = metadata == null ? null : metadata.version();
         // version is null only with some nasty flags, we don't care about ordering then
         if (version != null) {
            CompletableFuture<Object> future = checkLockAndStore(rCtx, command, cacheEntry.getKey(), version, Long.MIN_VALUE);
            if (future != null) {
               if (futures == null) {
                  futures = new ArrayList<>(lookedUpEntries.size());
               }
               futures.add(future);
            }
         } else {
            storeEntry(rCtx, cacheEntry.getKey(), command);
         }
      }
      if (futures == null) {
         if (getStatisticsEnabled())
            cacheStores.getAndAdd(lookedUpEntries.size());
         return null;
      } else {
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
         scheduleTimeout(allFuture, deadline, lookedUpEntries.keySet());
         return allFuture.thenCompose(ignored -> {
            if (getStatisticsEnabled())
               cacheStores.getAndAdd(lookedUpEntries.size());
            return null;
         });
      }
   };
   private ReturnHandler invalidateReturnHandler = (rCtx, rCommand, rv, throwable) -> {
      InvalidateVersionsCommand command = (InvalidateVersionsCommand) rCommand;
      Object[] keys = command.getKeys();
      List<CompletableFuture<Object>> futures = null;
      long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
      for (int i = 0; i < keys.length; ++i) {
         Object key = keys[i];
         if (key == null) break;
         CompletableFuture<Object> future = checkLockAndRemove(key);
         if (future != null) {
            if (futures == null) {
               futures = new ArrayList<>(keys.length);
            }
            futures.add(future);
         }
      }
      if (futures == null) {
         return null;
      } else if (futures.size() == 1) {
         return futures.get(0);
      } else {
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
         scheduleTimeout(allFuture, deadline, Arrays.toString(keys));
         return allFuture.thenCompose(ignored -> null);
      }
   };

   private void scheduleTimeout(CompletableFuture<?> future, long deadline, Object keys) {
      timeoutExecutor.schedule(() -> future.completeExceptionally(
         log.unableToAcquireLock(Util.prettyPrintTime(lockTimeout, TimeUnit.NANOSECONDS), keys, null, null)),
         timeService.remainingTime(deadline, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(ClusteringDependentLogic cdl, DataContainer dataContainer, TimeService timeService,
                      @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.cdl = cdl;
      this.dataContainer = dataContainer;
      this.timeService = timeService;
      this.timeoutExecutor = timeoutExecutor;
   }

   public void start() {
      super.start();
      // TODO: scattered cache cannot configure locking via XML
      this.lockTimeout = TimeUnit.MILLISECONDS.toNanos(cacheConfiguration.locking().lockAcquisitionTimeout());
   }

   private CompletableFuture<Object> checkLockAndStore(InvocationContext ctx, FlagAffectedCommand command, Object key, EntryVersion version, long deadline) {
      ByRef<CompletableFuture<Void>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<Void>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if there is no entry in DC, the value was either already passivated or the command did not write anything
         // (that shouldn't happen as we check if the command is successful)
         if (oldEntry != null) {
            Metadata oldMetadata;
            EntryVersion oldVersion;
            if ((oldMetadata = oldEntry.getMetadata()) == null || (oldVersion = oldMetadata.version()) == null) {
               // the record was written without version?
               lock(k, lockedFuture, waitFuture);
            } else {
               InequalVersionComparisonResult result = oldVersion.compareTo(version);
               switch (result) {
                  case AFTER:
                     // just ignore the write
                     break;
                  case EQUAL:
                     lock(k, lockedFuture, waitFuture);
                     break;
                  case BEFORE: // the actual version was not committed but we're here?
                  case CONFLICTING: // not used with numeric versions
                  default:
                     throw new IllegalStateException("DC version: " + oldVersion + ", cmd version " + version);
               }
            }
         }
         return oldEntry;
      });
      CompletableFuture<Void> wf = waitFuture.get();
      if (wf != null) {
         // multi-entry command schedule the timeout for all futures together. The composed futures can still come into
         // effect after the command throws timeout, but these will probably just find out newer versions in DC.
         if (deadline > Long.MIN_VALUE) {
            scheduleTimeout(wf, deadline, key);
         }
         return wf.thenCompose(ignored -> checkLockAndStore(ctx, command, key, version, deadline));
      }
      CompletableFuture<Void> lf = lockedFuture.get();
      if (lf != null) {
         try {
            storeEntry(ctx, key, command);
            if (getStatisticsEnabled())
               cacheStores.incrementAndGet();
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("Noone but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return null;
   }

   private CompletableFuture<Object> checkLockAndRemove(Object key) {
      ByRef<CompletableFuture<Void>> lockedFuture = new ByRef<>(null);
      ByRef<CompletableFuture<Void>> waitFuture = new ByRef<>(null);
      dataContainer.compute(key, (k, oldEntry, factory) -> {
         // if the entry is not null, the entry was not invalidated, or it was already written again
         if (oldEntry == null) {
            lock(k, lockedFuture, waitFuture);
         }
         return oldEntry;
      });
      CompletableFuture<Void> wf = waitFuture.get();
      if (wf != null) {
         return wf.thenCompose(ignored -> checkLockAndRemove(key));
      }
      CompletableFuture<Void> lf = lockedFuture.get();
      if (lf != null) {
         try {
            PersistenceManager.AccessMode mode = cdl.localNodeIsPrimaryOwner(key) ? PersistenceManager.AccessMode.BOTH : PersistenceManager.AccessMode.PRIVATE;
            persistenceManager.deleteFromAllStores(key, mode);
         } finally {
            if (!locks.remove(key, lf)) {
               throw new IllegalStateException("Noone but me should be able to replace the future");
            }
            lf.complete(null);
         }
      }
      return null;
   }

   private void lock(Object key, ByRef<CompletableFuture<Void>> lockedFuture, ByRef<CompletableFuture<Void>> waitFuture) {
      CompletableFuture<Void> myFuture = new CompletableFuture<>();
      CompletableFuture<Void> prevFuture = locks.putIfAbsent(key, myFuture);
      if (prevFuture == null) {
         lockedFuture.set(myFuture);
      } else {
         waitFuture.set(prevFuture);
      }
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
      throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return ctx.onReturn(manyDataReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return ctx.onReturn(dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return ctx.onReturn(manyDataReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return ctx.onReturn(manyDataReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return ctx.onReturn(manyDataReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return ctx.onReturn(manyDataReturnHandler);
   }

   @Override
   public Object visitInvalidateVersionsCommand(InvocationContext ctx, InvalidateVersionsCommand command) throws Throwable {
      return ctx.onReturn(invalidateReturnHandler);
   }

   @Override
   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !cdl.localNodeIsPrimaryOwner(key) || command.hasFlag(Flag.SKIP_SHARED_CACHE_STORE);
   }
}
