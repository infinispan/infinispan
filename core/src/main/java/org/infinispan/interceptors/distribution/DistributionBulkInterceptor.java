package org.infinispan.interceptors.distribution;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.stream.impl.RemovableCloseableIterator;
import org.infinispan.stream.impl.RemovableIterator;
import org.infinispan.stream.impl.tx.TxClusterStreamManager;
import org.infinispan.stream.impl.tx.TxDistributedCacheStream;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

/**
 * Interceptor that handles bulk entrySet and keySet commands when using in a distributed/replicated environment.
 * This interceptor produces backing collections for either method and a distributed stream for either which leverages
 * distributed processing through the cluster.
 * @param <K> The key type of entries
 * @param <V> The value type of entries
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class DistributionBulkInterceptor<K, V> extends CommandInterceptor {
   private Cache<K, V> cache;

   @Inject
   public void inject(Cache<K, V> cache) {
      this.cache = cache;
   }

   @Override
   public CacheSet<CacheEntry<K, V>> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      CacheSet<CacheEntry<K, V>> entrySet = (CacheSet<CacheEntry<K, V>>) super.visitEntrySetCommand(ctx, command);
      if (!command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (ctx.isInTxScope()) {
            return new TxBackingEntrySet<>(getCacheWithFlags(cache, command), entrySet, command,
                    (LocalTxInvocationContext) ctx);
         } else {
            return new BackingEntrySet<>(getCacheWithFlags(cache, command), entrySet, command);
         }
      }
      return entrySet;
   }

   protected static class BackingEntrySet<K, V> extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V>
           implements CacheSet<CacheEntry<K, V>> {
      protected final CacheSet<CacheEntry<K, V>> entrySet;
      protected final LocalFlagAffectedCommand command;

      private BackingEntrySet(Cache cache, CacheSet<CacheEntry<K, V>> entrySet, LocalFlagAffectedCommand command) {
         super(cache);
         this.entrySet = entrySet;
         this.command = command;
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         return new CloseableIteratorMapper<>(new RemovableCloseableIterator<>(Closeables.iterator(stream()), cache,
                 CacheEntry::getKey), e -> new EntryWrapper<>(cache, e));
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
         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<CacheEntry<K, V>>(
                 cache.getCacheManager().getAddress(), false, advancedCache.getDistributionManager(),
                 () -> entrySet.stream(), registry.getComponent(ClusterStreamManager.class),
                 !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry) {
            @Override
            public Iterator<CacheEntry<K, V>> iterator() {
               if (intermediateOperations.isEmpty()) {
                  return new RemovableIterator<>(super.iterator(), cache, e -> e.getKey());
               }
               return super.iterator();
            }
         };
         return applyTimeOut(cacheStream, cache);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         CacheStream<CacheEntry<K, V>> cacheStream = new DistributedCacheStream<>(cache.getCacheManager().getAddress(),
                 true, advancedCache.getDistributionManager(), () -> entrySet.parallelStream(),
                 registry.getComponent(ClusterStreamManager.class), !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry);
         return applyTimeOut(cacheStream, cache);
      }
   }

   protected static class TxBackingEntrySet<K, V> extends BackingEntrySet<K, V> {
      private final LocalTxInvocationContext ctx;

      private TxBackingEntrySet(Cache cache, CacheSet<CacheEntry<K, V>> entrySet, LocalFlagAffectedCommand command,
                                LocalTxInvocationContext ctx) {
         super(cache, entrySet, command);
         this.ctx = ctx;
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         ClusterStreamManager<K> realManager = registry.getComponent(ClusterStreamManager.class);
         TxClusterStreamManager<K> txManager = new TxClusterStreamManager<>(realManager, ctx, dm.getConsistentHash());

         CacheStream<CacheEntry<K, V>> cacheStream = new TxDistributedCacheStream<>(cache.getCacheManager().getAddress(),
                 false, dm, () -> entrySet.stream(), txManager, !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry, ctx);
         return applyTimeOut(cacheStream, cache);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         ClusterStreamManager<K> realManager = registry.getComponent(ClusterStreamManager.class);
         TxClusterStreamManager<K> txManager = new TxClusterStreamManager<>(realManager, ctx, dm.getConsistentHash());

         CacheStream<CacheEntry<K, V>> cacheStream = new TxDistributedCacheStream<>(cache.getCacheManager().getAddress(),
                 true, dm, () -> entrySet.parallelStream(), txManager, !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry, ctx);
         return applyTimeOut(cacheStream, cache);
      }
   }

   private static <C> CacheStream<C> applyTimeOut(CacheStream<C> stream, Cache<?, ?> cache) {
      return stream.timeout(cache.getCacheConfiguration().clustering().remoteTimeout(),
              TimeUnit.MILLISECONDS);
   }

   /**
    * Wrapper for CacheEntry(s) that can be used to update the cache when it's value is set.
    * @param <K> The key type
    * @param <V> The value type
    */
   private static class EntryWrapper<K, V> extends ForwardingCacheEntry<K, V> {
      private final Cache<K, V> cache;
      private final CacheEntry<K, V> entry;

      public EntryWrapper(Cache<K, V> cache, CacheEntry<K, V> entry) {
         this.cache = cache;
         this.entry = entry;
      }

      @Override
      protected CacheEntry<K, V> delegate() {
         return entry;
      }

      @Override
      public V setValue(V value) {
         cache.put(entry.getKey(), value);
         return super.setValue(value);
      }
   }

   @Override
   public CacheSet<K> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      if (!command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (ctx.isInTxScope()) {
            return new TxBackingKeySet<>(getCacheWithFlags(cache, command), cache.getAdvancedCache().withFlags(
                    Flag.CACHE_MODE_LOCAL).cacheEntrySet(), command, (LocalTxInvocationContext) ctx);
         } else {
            return new BackingKeySet<>(getCacheWithFlags(cache, command), cache.getAdvancedCache().withFlags(
                    Flag.CACHE_MODE_LOCAL).cacheEntrySet(), command);
         }
      }
      return (CacheSet<K>) super.visitKeySetCommand(ctx, command);
   }

   protected static class BackingKeySet<K, V> extends AbstractCloseableIteratorCollection<K, K, V>
           implements CacheSet<K> {
      protected final CacheSet<CacheEntry<K, V>> entrySet;
      protected final LocalFlagAffectedCommand command;

      public BackingKeySet(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> entrySet, LocalFlagAffectedCommand command) {
         super(cache);
         this.entrySet = entrySet;
         this.command = command;
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new RemovableCloseableIterator(Closeables.iterator(stream()), cache, Function.identity());
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
         return new DistributedCacheStream<K>(cache.getCacheManager().getAddress(), false,
                 advancedCache.getDistributionManager(), () -> entrySet.stream(),
                 registry.getComponent(ClusterStreamManager.class), !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry,
                 StreamMarshalling.entryToKeyFunction()) {
            @Override
            public Iterator<K> iterator() {
               // The act of mapping to key requires 1 intermediate operation
               if (intermediateOperations.size() == 1) {
                  return new RemovableIterator<>(super.iterator(), cache, Function.identity());
               }
               return super.iterator();
            }
         };
      }

      @Override
      public CacheStream<K> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         return new DistributedCacheStream<>(cache.getCacheManager().getAddress(), true,
                 advancedCache.getDistributionManager(), () -> entrySet.parallelStream(),
                 registry.getComponent(ClusterStreamManager.class), !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry,
                 StreamMarshalling.entryToKeyFunction());
      }
   }

   private static class TxBackingKeySet<K, V> extends BackingKeySet<K, V> {
      private final LocalTxInvocationContext ctx;

      public TxBackingKeySet(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> entrySet, LocalFlagAffectedCommand command,
                             LocalTxInvocationContext ctx) {
         super(cache, entrySet, command);
         this.ctx = ctx;
      }

      @Override
      public CacheStream<K> stream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         ClusterStreamManager<K> realManager = registry.getComponent(ClusterStreamManager.class);
         TxClusterStreamManager<K> txManager = new TxClusterStreamManager<>(realManager, ctx, dm.getConsistentHash());

         return new TxDistributedCacheStream<>(cache.getCacheManager().getAddress(), false,
                 dm, () -> entrySet.stream(), txManager, !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry,
                 StreamMarshalling.entryToKeyFunction(), ctx);
      }

      @Override
      public CacheStream<K> parallelStream() {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         DistributionManager dm = advancedCache.getDistributionManager();
         ComponentRegistry registry = advancedCache.getComponentRegistry();
         ClusterStreamManager<K> realManager = registry.getComponent(ClusterStreamManager.class);
         TxClusterStreamManager<K> txManager = new TxClusterStreamManager<>(realManager, ctx, dm.getConsistentHash());

         return new TxDistributedCacheStream<>(cache.getCacheManager().getAddress(), true,
                 dm, () -> entrySet.parallelStream(), txManager, !command.hasFlag(Flag.SKIP_CACHE_LOAD),
                 cache.getCacheConfiguration().clustering().stateTransfer().chunkSize(),
                 registry.getComponent(Executor.class, ASYNC_OPERATIONS_EXECUTOR), registry,
                 StreamMarshalling.entryToKeyFunction(), ctx);
      }
   }
}
