package org.infinispan.interceptors.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.KeyValueMetadataSizeCalculator;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that prevents the cache from inserting too many entries over a configured maximum amount.
 * This interceptor assumes that there is a transactional cache without one phase commit semantics.
 * @author wburns
 * @since 9.0
 */
public class TransactionalExceptionEvictionInterceptor extends DDAsyncInterceptor implements
      InternalExpirationManager.ExpirationConsumer<Object, Object>, Consumer<Iterable<InternalCacheEntry<Object, Object>>> {
   private static final Log log = LogFactory.getLog(TransactionalExceptionEvictionInterceptor.class);

   private final AtomicLong currentSize = new AtomicLong();
   private final ConcurrentMap<GlobalTransaction, Long> pendingSize = new ConcurrentHashMap<>();
   private MemoryConfiguration memoryConfiguration;
   private InternalDataContainer<Object, Object> container;
   private DistributionManager dm;
   private long maxSize;
   private long minSize;
   private KeyValueMetadataSizeCalculator<Object, Object> calculator;
   private InternalExpirationManager<Object, Object> expirationManager;

   private final InvocationSuccessAction<RemoveExpiredCommand> removeExpiredAccept = this::removeExpiredAccept;

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
   public void inject(Configuration config, InternalDataContainer<Object, Object> dataContainer,
         KeyValueMetadataSizeCalculator<Object, Object> calculator, DistributionManager dm,
         InternalExpirationManager<Object, Object> expirationManager) {
      this.memoryConfiguration = config.memory();
      this.container = dataContainer;
      this.maxSize = config.memory().size();
      this.calculator = calculator;
      this.dm = dm;
      this.expirationManager = expirationManager;
   }

   @Start
   public void start() {
      if (memoryConfiguration.storageType() == StorageType.OFF_HEAP && memoryConfiguration.evictionType() == EvictionType.MEMORY) {
         // TODO: this is technically not correct - as the underlying map can resize (also it doesn't take int account
         // we have a different map for each segment when not in LOCAL mode
         minSize = UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(OffHeapConcurrentMap.INITIAL_SIZE << 3);
         currentSize.set(minSize);
      }

      container.addRemovalListener(this);
      expirationManager.addInternalListener(this);
   }

   @Stop
   public void stop() {
      container.removeRemovalListener(this);
      expirationManager.removeInternalListener(this);
   }

   @Override
   public void expired(Object key, Object value, Metadata metadata, PrivateMetadata privateMetadata) {
      // If this is null it means it was from the store, so we don't care about that
      if (value != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Key %s found to have expired", key);
         }
         adjustSize(- calculator.calculateSize(key, value, metadata, privateMetadata));
      }
   }

   @Override
   public void accept(Iterable<InternalCacheEntry<Object, Object>> entries) {
      long changeAmount = 0;
      for (InternalCacheEntry<Object, Object> entry : entries) {
         changeAmount -= calculator.calculateSize(entry.getKey(), entry.getValue(), entry.getMetadata(), entry.getInternalMetadata());
      }
      if (changeAmount != 0) {
         adjustSize(changeAmount);
      }
   }

   private boolean adjustSize(long amount) {
      while (true) {
         long size = currentSize.get();
         long targetSize = size + amount;
         if (targetSize <= maxSize) {
            if (currentSize.compareAndSet(size, size + amount)) {
               if (log.isTraceEnabled()) {
                  log.tracef("Adjusted exception based size by %d to %d", amount, size + amount);
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
      /*
       * State transfer uses invalidate command to remove entries that are outside of the tx context
       */
      Object[] keys = command.getKeys();
      long changeAmount = 0;
      for (Object key : keys) {
         InternalCacheEntry<Object, Object> entry = container.peek(key);
         if (entry != null) {
            changeAmount -= calculator.calculateSize(key, entry.getValue(), entry.getMetadata(), entry.getInternalMetadata());
         }
      }
      if (changeAmount != 0) {
         adjustSize(changeAmount);
      }
      return super.visitInvalidateCommand(ctx, command);
   }

   // Remove Expired is not transactional
   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) {
      Object key = command.getKey();
      // Skip adding changeAmount if originator is not primary
      if (ctx.isOriginLocal() && dm != null && !dm.getCacheTopology().getSegmentDistribution(command.getSegment()).isPrimary()) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenAccept(ctx, command, removeExpiredAccept);
   }

   private void removeExpiredAccept(InvocationContext rCtx, RemoveExpiredCommand rCommand, Object rValue) {
      Object rKey = rCommand.getKey();
      if (rCommand.isSuccessful()) {
         if (dm == null || dm.getCacheTopology().getSegmentDistribution(rCommand.getSegment()).isWriteOwner()) {
            MVCCEntry<?, ?> entry = (MVCCEntry<?, ?>) rCtx.lookupEntry(rKey);
            if (log.isTraceEnabled()) {
               log.tracef("Key %s was removed via expiration", rKey);
            }

            long changeAmount = -calculator.calculateSize(rKey, entry.getOldValue(), entry.getOldMetadata(), entry.getInternalMetadata());
            if (changeAmount != 0 && !adjustSize(changeAmount)) {
               throw CONTAINER.containerFull(maxSize);
            }
         }
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
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

      long changeAmount = 0;
      for (Object key : modifiedKeys) {
         if (dm == null || dm.getCacheTopology().isWriteOwner(key)) {
            CacheEntry<Object, Object> entry = ctx.lookupEntry(key);
            if (entry.isRemoved()) {
               // Need to subtract old value here
               InternalCacheEntry<Object, Object> containerEntry = container.peek(key);
               Object value = containerEntry != null ? containerEntry.getValue() : null;
               if (value != null) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Key %s was removed", key);
                  }
                  changeAmount -= calculator.calculateSize(key, value, entry.getMetadata(), entry.getInternalMetadata());
               }
            } else {
               // We check the container directly - this is to handle entries that are expired as the command
               // won't think it replaced a value
               InternalCacheEntry<Object, Object> containerEntry = container.peek(key);
               if (log.isTraceEnabled()) {
                  log.tracef("Key %s was put into cache, replacing existing %s", key, containerEntry != null);
               }
               // Create and replace both add for the new value
               changeAmount += calculator.calculateSize(key, entry.getValue(), entry.getMetadata(), entry.getInternalMetadata());
               // Need to subtract old value here
               if (containerEntry != null) {
                  changeAmount -= calculator.calculateSize(key, containerEntry.getValue(), containerEntry.getMetadata(), containerEntry.getInternalMetadata());
               }
            }
         }
      }

      if (changeAmount != 0 && !adjustSize(changeAmount)) {
         throw CONTAINER.containerFull(maxSize);
      }

      if (!command.isOnePhaseCommit()) {
         pendingSize.put(ctx.getGlobalTransaction(), changeAmount);
      }

      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      Long size = pendingSize.remove(ctx.getGlobalTransaction());
      if (size != null) {
         long newSize = currentSize.addAndGet(-size);
         if (log.isTraceEnabled()) {
            log.tracef("Rollback encountered subtracting exception size by %d to %d", size.longValue(), newSize);
         }
      }
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      pendingSize.remove(ctx.getGlobalTransaction());
      return super.visitCommitCommand(ctx, command);
   }
}
