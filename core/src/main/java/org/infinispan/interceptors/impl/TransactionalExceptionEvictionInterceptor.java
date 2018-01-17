package org.infinispan.interceptors.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.Cache;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.KeyValueMetadataSizeCalculator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that prevents the cache from inserting too many entries over a configured maximum amount.
 * This interceptor assumes that there is a transactional cache without one phase commit semantics.
 * @author wburns
 * @since 9.0
 */
@Listener(observation = Listener.Observation.POST)
public class TransactionalExceptionEvictionInterceptor extends DDAsyncInterceptor {
   private final static Log log = LogFactory.getLog(TransactionalExceptionEvictionInterceptor.class);
   private final static boolean isTrace = log.isTraceEnabled();

   private final AtomicLong currentSize = new AtomicLong();
   private final ConcurrentMap<GlobalTransaction, Long> pendingSize = new ConcurrentHashMap<>();
   private MemoryConfiguration memoryConfiguration;
   private Cache cache;
   private DataContainer container;
   DistributionManager dm;
   private long maxSize;
   private long minSize;
   private KeyValueMetadataSizeCalculator calculator;

   public long getCurrentSize() {
      return currentSize.get();
   }

   public long getMaxSize() {
      return maxSize;
   }

   public long getMinSize() {
      return minSize;
   }

   public long pendingTransactionCount() {
      return pendingSize.size();
   }

   @Inject
   public void inject(Configuration config, Cache cache,
         DataContainer dataContainer, KeyValueMetadataSizeCalculator calculator, DistributionManager dm) {
      this.memoryConfiguration = config.memory();
      this.cache = cache;
      this.container = dataContainer;
      this.maxSize = config.memory().size();
      this.calculator = calculator;
      this.dm = dm;
   }

   @Start
   public void start() {
      if (memoryConfiguration.storageType() == StorageType.OFF_HEAP) {
         minSize = UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(memoryConfiguration.addressCount() << 3);
         currentSize.set(minSize);
      }

      // Local caches just remove the entry, so we have to listen for those events
      if (!cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
         // We want the raw values and no transformations for our listener
         // We can't use AbstractDelegatingCache.unwrapCache(cache) as this would give us byte[] instead of WrappedByteArray
         cache.getAdvancedCache().withEncoding(IdentityEncoder.class).withWrapping(IdentityWrapper.class).addListener(this);
      }
   }

   @CacheEntryExpired
   public void entryExpired(CacheEntryExpiredEvent event) {
      // If this is null it means it was from the store, so we don't care about that
      if (event.getValue() != null) {
         increaseSize(- calculator.calculateSize(event.getKey(), event.getValue(), event.getMetadata()));
      }
   }

   private boolean increaseSize(long increaseAmount) {
      while (true) {
         long size = currentSize.get();
         long targetSize = size + increaseAmount;
         if (targetSize <= maxSize) {
            if (currentSize.compareAndSet(size, size + increaseAmount)) {
               if (isTrace) {
                  log.tracef("Increased exception based size by %d to %d", increaseAmount, size + increaseAmount);
               }
               return true;
            }
         } else {
            return false;
         }
      }
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      /**
       * State transfer uses invalidate command to remove entries that are outside of the tx context
       */
      Object[] keys = command.getKeys();
      long changeAmount = 0;
      for (Object key : keys) {
         InternalCacheEntry entry = container.peek(key);
         if (entry != null) {
            changeAmount -= calculator.calculateSize(key, entry.getValue(), entry.getMetadata());
         }
      }
      if (changeAmount != 0) {
         increaseSize(changeAmount);
      }
      return super.visitInvalidateCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (isTrace) {
         log.tracef("Clear command encountered, resetting size to %d", minSize);
      }
      // Clear is never invoked in the middle of a transaction with others so just set the size
      currentSize.set(minSize);
      return super.visitClearCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      // If we just invoke ctx.getModifications it won't return the modifications for REPL state transfer
      List<WriteCommand> modifications = ctx.getCacheTransaction().getAllModifications();
      Set<Object> modifiedKeys = new HashSet<>();
      for (WriteCommand modification : modifications) {
         modifiedKeys.addAll(modification.getAffectedKeys());
      }

      Map<Object, CacheEntry> entries = ctx.getLookedUpEntries();
      long changeAmount = 0;
      for (Object key : modifiedKeys) {
         if (dm == null || dm.getCacheTopology().isWriteOwner(key)) {
            CacheEntry entry = entries.get(key);
            if (entry.isRemoved()) {
               // Need to subtract old value here
               InternalCacheEntry containerEntry = container.peek(key);
               Object value = containerEntry != null ? containerEntry.getValue() : null;
               if (value != null) {
                  changeAmount -= calculator.calculateSize(key, value, entry.getMetadata());
               }
            } else {
               // Create and replace both add for the new value
               changeAmount += calculator.calculateSize(key, entry.getValue(), entry.getMetadata());
               if (!entry.isCreated()) {
                  // Need to subtract old value here
                  InternalCacheEntry containerEntry = container.peek(key);
                  if (containerEntry != null) {
                     changeAmount -= calculator.calculateSize(key, containerEntry.getValue(), containerEntry.getMetadata());
                  }
               }
            }
         }
      }

      if (!increaseSize(changeAmount)) {
         throw log.containerFull(maxSize);
      }

      pendingSize.put(ctx.getGlobalTransaction(), changeAmount);

      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      Long size = pendingSize.remove(ctx.getGlobalTransaction());
      if (size != null) {
         if (isTrace) {
            log.tracef("Rollback encountered subtracting exception size by %d", size);
         }
         currentSize.addAndGet(-size);
      }
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      pendingSize.remove(ctx.getGlobalTransaction());
      return super.visitCommitCommand(ctx, command);
   }
}
