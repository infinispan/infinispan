package org.infinispan.interceptors.distribution;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.Caches;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.util.EntryWrapper;

/**
 * Interceptor that handles bulk entrySet and keySet commands when using in a distributed/replicated environment.
 * This interceptor produces backing collections for either method and a distributed stream for either which leverages
 * distributed processing through the cluster.
 * @param <K> The key type of entries
 * @param <V> The value type of entries
 */
public class DistributionBulkInterceptor<K, V> extends DDAsyncInterceptor {
   @Inject ComponentRef<Cache<K, V>> cache;

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         return super.visitEntrySetCommand(ctx, command);
      }
      // We just set it, we always wrap our iterator and support removal
      command.addFlags(FlagBitSets.REMOTE_ITERATION);

      return invokeNextThenApply(ctx, command, (rCtx, entrySetCommand, rv) -> {
         CacheSet<CacheEntry<K, V>> entrySet = (CacheSet<CacheEntry<K, V>>) rv;
         if (rCtx.isInTxScope()) {
            entrySet = new TxBackingEntrySet<>(Caches.getCacheWithFlags(cache.wired(), entrySetCommand), entrySet, entrySetCommand,
                                               (LocalTxInvocationContext) rCtx);
         } else {
            entrySet = new BackingEntrySet<>(Caches.getCacheWithFlags(cache.wired(), entrySetCommand), entrySet, entrySetCommand);
         }
         return entrySet;
      });
   }

   protected static class BackingEntrySet<K, V> extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V>
           implements CacheSet<CacheEntry<K, V>> {
      protected final CacheSet<CacheEntry<K, V>> entrySet;
      protected final FlagAffectedCommand command;

      private BackingEntrySet(Cache cache, CacheSet<CacheEntry<K, V>> entrySet, FlagAffectedCommand command) {
         super(cache.getAdvancedCache().withStorageMediaType());
         this.entrySet = entrySet;
         this.command = command;
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         return new IteratorMapper<>(new RemovableCloseableIterator<>(Closeables.iterator(stream()),
               e -> cache.remove(e.getKey(), e.getValue())), e -> new EntryWrapper<>(cache, e));
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         return Closeables.spliterator(stream());
      }

      @Override
      public boolean contains(Object o) {
         Map.Entry entry = toEntry(o);
         if (entry != null) {
            V value = cache.get(entry.getKey());
            return value != null && value.equals(entry.getValue());
         }
         return false;
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry entry = toEntry(o);
         if (entry != null) {
            return cache.remove(entry.getKey(), entry.getValue());
         }
         return false;
      }

      private Map.Entry<K, V> toEntry(Object obj) {
         if (obj instanceof Map.Entry) {
            return (Map.Entry) obj;
         } else {
            return null;
         }
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<>(
               cache.getCacheManager().getAddress(), false, advancedCache.getDistributionManager(),
               null, entrySet::stream,
               !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
               cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
               registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
               StreamMarshalling.entryToKeyFunction());
         return applyTimeOut(cacheStream, cache);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<CacheEntry<K, V>, CacheEntry<K, V>>(
               cache.getCacheManager().getAddress(), true, advancedCache.getDistributionManager(),
               null, entrySet::parallelStream,
               !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
               cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
               registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
               StreamMarshalling.entryToKeyFunction());
         return applyTimeOut(cacheStream, cache);
      }
   }

   protected static class TxBackingEntrySet<K, V> extends BackingEntrySet<K, V> {
      private final LocalTxInvocationContext ctx;

      private TxBackingEntrySet(Cache cache, CacheSet<CacheEntry<K, V>> entrySet, FlagAffectedCommand command,
                                LocalTxInvocationContext ctx) {
         super(cache, entrySet, command);
         this.ctx = ctx;
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();

         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<>(cache.getCacheManager().getAddress(),
               false, dm, ctx, entrySet::stream, !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
               cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
               registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
               StreamMarshalling.entryToKeyFunction());
         return applyTimeOut(cacheStream, cache);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<>(cache.getCacheManager().getAddress(),
               true, dm, ctx, entrySet::parallelStream, !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
               cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
               registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
               StreamMarshalling.entryToKeyFunction());
         return applyTimeOut(cacheStream, cache);
      }
   }

   private static <C> CacheStream<C> applyTimeOut(CacheStream<C> stream, Cache<?, ?> cache) {
      return stream.timeout(cache.getCacheConfiguration().clustering().remoteTimeout(),
              TimeUnit.MILLISECONDS);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         return super.visitKeySetCommand(ctx, command);
      }
      // We just set it, we always wrap our iterator and support removal
      command.addFlags(FlagBitSets.REMOTE_ITERATION);

      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         CacheSet<K> keySet = (CacheSet<K>) rv;
         if (rCtx.isInTxScope()) {
            keySet = new TxBackingKeySet<>(Caches.getCacheWithFlags(cache.wired(), rCommand), keySet, rCommand,
                                           (LocalTxInvocationContext) rCtx);
         } else {
            keySet = new BackingKeySet<>(Caches.getCacheWithFlags(cache.wired(), rCommand), keySet, rCommand);
         }
         return keySet;
      });
   }

   protected static class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V>
           implements CacheSet<K> {
      protected final CacheSet<K> keySet;
      protected final FlagAffectedCommand command;

      public BackingKeySet(Cache<K, V> cache, CacheSet<K> keySet, FlagAffectedCommand command) {
         super(cache.getAdvancedCache().withStorageMediaType());
         this.keySet = keySet;
         this.command = command;
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new RemovableCloseableIterator<>(Closeables.iterator(stream()), cache::remove);
      }

      @Override
      public boolean contains(Object o) {
         return cache.containsKey(o);
      }

      @Override
      public boolean remove(Object o) {
         return cache.remove(o) != null;
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return Closeables.spliterator(iterator(), Long.MAX_VALUE,
                 Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL);
      }

      @Override
      public CacheStream<K> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         return new DistributedCacheStream<>(cache.getCacheManager().getAddress(), false,
                 advancedCache.getDistributionManager(), null, keySet::stream,
                 !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry, null);
      }

      @Override
      public CacheStream<K> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         return new DistributedCacheStream<>(cache.getCacheManager().getAddress(), true,
                 advancedCache.getDistributionManager(), null, keySet::parallelStream,
                 !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry, null);
      }
   }

   private static class TxBackingKeySet<K, V> extends BackingKeySet<K, V> {
      private final LocalTxInvocationContext ctx;

      public TxBackingKeySet(Cache<K, V> cache, CacheSet<K> keySet, FlagAffectedCommand command,
                             LocalTxInvocationContext ctx) {
         super(cache, keySet, command);
         this.ctx = ctx;
      }

      @Override
      public CacheStream<K> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();

         return new DistributedCacheStream<>(cache.getCacheManager().getAddress(), false,
                 dm, ctx, keySet::stream, !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
                 null);
      }

      @Override
      public CacheStream<K> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();

         return new DistributedCacheStream<>(cache.getCacheManager().getAddress(), true,
                 dm, ctx, keySet::parallelStream, !command.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, NON_BLOCKING_EXECUTOR), registry,
                 null);
      }
   }
}
