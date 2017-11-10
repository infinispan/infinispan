package org.infinispan.interceptors.impl;

import org.infinispan.commands.DataCommand;
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
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Similar to {@link DistCacheWriterInterceptor} but as commands are not forwarded from primary owner
 * so we just write to the store all the time (with non-shared interceptors).
 *
 * Uses its own locking scheme to order writes into the store - if there is a newer write into DC,
 * the older write is ignored. This could improve contented case, but has one strange property:
 * Assume CS contains V1, T2 writes V2 and T3 writes V3; if T2 receives version and stores into DC
 * before T3 but arrives here after T3 the V2 write into CS is ignored but the operation is confirmed
 * as if it was persisted correctly.
 * If T3 then (spuriously) fails to write into the store, V1 stays in CS but the error is reported
 * only to T3 - that means that T3 effectivelly rolled back V2 into V1 (and reported this as an error).
 * This may surprise some users.
 *
 * Reads can be blocked by ongoing writes, though; when T2 finishes and then the application attempts
 * to read the value, and removal from T3 is not complete yet the read would not find the value in DC
 * (because it was removed by T3) but could load V1 from cache store. Therefore, read must wait until
 * the current write (that could have interacted with previous write) finishes.
 *
 * TODO: block writes until other write completes, and don't block reads
 *
 * However, blocking reads in cachestore is not something unusual; the DC lock is acquired when writing
 * the cache store during eviction/passivation, or during write skew checks in other modes as well.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredCacheWriterInterceptor extends CacheWriterInterceptor {

   private static final Log log = LogFactory.getLog(DistCacheWriterInterceptor.class);

   @Inject private DistributionManager dm;
   @Inject private TimeService timeService;
   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   private ScheduledExecutorService timeoutExecutor;
   @Inject private OrderedUpdatesManager orderedUpdatesManager;

   private long lockTimeout;

   private final InvocationSuccessFunction handleDataWriteReturn = this::handleDataWriteReturn;
   private final InvocationSuccessFunction handleManyWriteReturn = this::handleManyWriteReturn;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public void start() {
      super.start();
      this.lockTimeout = TimeUnit.MILLISECONDS.toNanos(cacheConfiguration.locking().lockAcquisitionTimeout());
   }

   private Object handleReadCommand(InvocationContext ctx, DataCommand command) {
      CompletableFuture<?> wf = orderedUpdatesManager.waitFuture(command.getKey());
      if (wf != null) {
         return asyncInvokeNext(ctx, command, wf);
      } else {
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      List<CompletableFuture<?>> wfs = null;
      for (Object key : command.getKeys()) {
         CompletableFuture<?> wf = orderedUpdatesManager.waitFuture(key);
         if (wf != null) {
            if (wfs == null) wfs = new ArrayList<>();
            wfs.add(wf);
         }
      }
      return asyncInvokeNext(ctx, command, wfs);
   }

   private Object handleDataWriteReturn(InvocationContext ctx, VisitableCommand command, Object rv) {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) command;
      Object key = dataWriteCommand.getKey();
      if (!isStoreEnabled(dataWriteCommand) || !dataWriteCommand.isSuccessful())
         return rv;

      CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (cacheEntry == null) {
         throw new IllegalStateException();
      }
      Metadata metadata = cacheEntry.getMetadata();
      EntryVersion version = metadata == null ? null : metadata.version();
      // version is null only with some nasty flags, we don't care about ordering then
      if (version != null) {
         long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
         CompletableFuture<?> future = orderedUpdatesManager.checkLockAndStore(key, version,
               wf -> scheduleTimeout(wf, deadline, key),
               k -> storeAndUpdateStats(ctx, k, dataWriteCommand));
         if (future == null) {
            return rv;
         } else {
            return asyncValue(future.thenApply(nil -> rv));
         }
      } else {
         storeAndUpdateStats(ctx, key, dataWriteCommand);
         return rv;
      }
   }

   private void storeAndUpdateStats(InvocationContext ctx, Object key, WriteCommand command) {
      storeEntry(ctx, key, command);
      if (getStatisticsEnabled())
         cacheStores.incrementAndGet();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
      throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   private Object handleManyWriteReturn(InvocationContext ctx, VisitableCommand rcommand, Object rv) {
      WriteCommand command = (WriteCommand) rcommand;
      if (!isStoreEnabled(command))
         return rv;

      Collection<?> keys = command.getAffectedKeys();

      long deadline = timeService.expectedEndTime(lockTimeout, TimeUnit.NANOSECONDS);
      WaitFutures waitFutures = new WaitFutures(lockTimeout, keys);

      List<CompletableFuture<?>> futures = null;
      for (Object key : keys) {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         Metadata metadata = cacheEntry.getMetadata();
         EntryVersion version = metadata == null ? null : metadata.version();
         // version is null only with some nasty flags, we don't care about ordering then
         if (version != null) {
            CompletableFuture<?> future = orderedUpdatesManager.checkLockAndStore(key, version,
                  wf -> {
                     CompletableFuture<?> cf = wf.thenAccept(nil -> {});
                     waitFutures.add(cf);
                     return cf;
                  },
                  k -> storeEntry(ctx, k, command));
            if (future != null && !future.isDone()) {
               if (futures == null) {
                  futures = new ArrayList<>(); // let's assume little contention
               }
               futures.add(future);
            }
         } else {
            storeEntry(ctx, cacheEntry.getKey(), command);
         }
      }
      if (futures == null) {
         if (getStatisticsEnabled())
            cacheStores.getAndAdd(keys.size());
         return rv;
      } else {
         ScheduledFuture<?> schedule = timeoutExecutor.schedule(waitFutures::cancel, timeService.remainingTime(deadline, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
         CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
         return asyncValue(allFuture.thenApply(nil -> {
            schedule.cancel(false);
            if (getStatisticsEnabled())
               cacheStores.getAndAdd(keys.size());
            return rv;
         }));
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleManyWriteReturn);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleDataWriteReturn);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleManyWriteReturn);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleManyWriteReturn);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleManyWriteReturn);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, handleManyWriteReturn);
   }

   @Override
   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      DistributionInfo info = dm.getCacheTopology().getDistribution(key);
      return !info.isPrimary() || command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }

   private CompletableFuture<?> scheduleTimeout(CompletableFuture<?> future, long deadline, Object key) {
      if (future.isDone()) {
         return future;
      }
      LockTimeoutFuture lockTimeoutFuture = new LockTimeoutFuture(lockTimeout, key);
      ScheduledFuture<?> schedule = timeoutExecutor.schedule(lockTimeoutFuture, timeService.remainingTime(deadline, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
      lockTimeoutFuture.setCancellation(schedule);
      future.whenComplete(lockTimeoutFuture);
      return lockTimeoutFuture;
   }

   public class LockTimeoutFuture extends CompletableFuture<Void> implements Runnable, BiConsumer<Object, Throwable> {
      private final Object key;
      private final long lockTimeout;
      private ScheduledFuture<?> cancellation;

      private LockTimeoutFuture(long lockTimeout, Object key) {
         this.lockTimeout = lockTimeout;
         this.key = key;
      }

      @Override
      public void run() {
         completeExceptionally(log.unableToAcquireLock(Util.prettyPrintTime(lockTimeout, TimeUnit.NANOSECONDS), key, null, null));
      }

      @Override
      public void accept(Object o, Throwable throwable) {
         cancellation.cancel(false);
         if (throwable != null) {
            completeExceptionally(throwable);
         } else {
            complete(null);
         }
      }

      public void setCancellation(ScheduledFuture<?> cancellation) {
         this.cancellation = cancellation;
      }
   }

   private static class WaitFutures {
      private final long lockTimeout;
      private final Collection<?> keys;
      private List<CompletableFuture<?>> futures;
      private boolean cancelled;

      private WaitFutures(long lockTimeout, Collection<?> keys) {
         this.lockTimeout = lockTimeout;
         this.keys = keys;
      }

      public synchronized void add(CompletableFuture<?> cf) {
         if (cancelled) {
            cf.completeExceptionally(log.unableToAcquireLock(Util.prettyPrintTime(lockTimeout, TimeUnit.NANOSECONDS), keys, null, null));
            return;
         }
         if (futures == null) futures = new ArrayList<>();
         futures.add(cf);
      }

      public synchronized void cancel() {
         for (CompletableFuture<?> cf : futures) {
            cf.completeExceptionally(log.unableToAcquireLock(Util.prettyPrintTime(lockTimeout, TimeUnit.NANOSECONDS), keys, null, null));
         }
         cancelled = true;
      }
   }
}
