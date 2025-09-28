package org.infinispan.client.hotrod;


import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.common.CloseableIterator;
import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncQuery;
import org.infinispan.api.sync.SyncStreamingCache;
import org.infinispan.api.sync.events.cache.SyncCacheEntryListener;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.reactivestreams.FlowAdapters;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncCache<K, V> implements SyncCache<K, V> {
   private final HotRod hotrod;
   private final InternalRemoteCache<K, V> remoteCache;
   private final OperationDispatcher dispatcher;

   HotRodSyncCache(HotRod hotrod, InternalRemoteCache<K, V> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
      this.dispatcher = remoteCache.getDispatcher();
   }

   // This method is blocking - but only invoked by user code
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   private <E> E blockingGet(Single<E> single) {
      return single.blockingGet();
   }

   @Override
   public String name() {
      return remoteCache.getName();
   }

   @Override
   public CacheConfiguration configuration() {
      return null;
   }

   @Override
   public SyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public CacheEntry<K, V> getEntry(K key, CacheOptions options) {
      return dispatcher.await(remoteCache.getEntry(key, options));
   }

   @Override
   public CacheEntry<K, V> put(K key, V value, CacheWriteOptions options) {
      return dispatcher.await(remoteCache.put(key, value, options));
   }

   @Override
   public void set(K key, V value, CacheWriteOptions options) {
      dispatcher.await(remoteCache.set(key, value, options));
   }

   @Override
   public CacheEntry<K, V> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return dispatcher.await(remoteCache.putIfAbsent(key, value, options));
   }

   @Override
   public boolean setIfAbsent(K key, V value, CacheWriteOptions options) {
      return dispatcher.await(remoteCache.setIfAbsent(key, value, options));
   }

   @Override
   public boolean replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return dispatcher.await(remoteCache.replace(key, value, version, options));
   }

   @Override
   public CacheEntry<K, V> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return dispatcher.await(remoteCache.getOrReplaceEntry(key, value, version, options));
   }

   @Override
   public boolean remove(K key, CacheOptions options) {
      return dispatcher.await(remoteCache.remove(key, options));
   }

   @Override
   public boolean remove(K key, CacheEntryVersion version, CacheOptions options) {
      return dispatcher.await(remoteCache.remove(key, version, options));
   }

   @Override
   public CacheEntry<K, V> getAndRemove(K key, CacheOptions options) {
      return dispatcher.await(remoteCache.getAndRemove(key, options));
   }

   @Override
   public CloseableIterable<K> keys(CacheOptions options) {
      return () -> toCloseableIterator(remoteCache.keys(options), 64);
   }

   @Override
   public CloseableIterable<CacheEntry<K, V>> entries(CacheOptions options) {
      return () -> toCloseableIterator(remoteCache.entries(options), 64);
   }

   private static <E> CloseableIterator<E> toCloseableIterator(Flow.Publisher<E> flow, int fetchSize) {
      Flowable<E> flowable = Flowable.fromPublisher(FlowAdapters.toPublisher(flow));
      @SuppressWarnings("checkstyle:forbiddenmethod")
      Iterable<E> iterable = flowable.blockingIterable(fetchSize);
      Iterator<E> iterator = iterable.iterator();
      return new CloseableIterator<>() {
         @Override
         public void close() {
            ((Disposable) iterator).dispose();
         }

         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public E next() {
            return iterator.next();
         }
      };
   }

   @Override
   public void putAll(Map<K, V> entries, CacheWriteOptions options) {
      dispatcher.await(remoteCache.putAll(entries, options));
   }

   @Override
   public Map<K, V> getAll(Set<K> keys, CacheOptions options) {
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(remoteCache.getAll(keys, options)))
            .collect(Collectors.toMap(CacheEntry::key, CacheEntry::value)));
   }

   @Override
   public Map<K, V> getAll(CacheOptions options, K[] keys) {
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(remoteCache.getAll(options, keys)))
            .collect(Collectors.toMap(CacheEntry::key, CacheEntry::value)));
   }

   @Override
   public Set<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(remoteCache.removeAll(keys, options)))
            .collect(Collectors.toSet()));
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(remoteCache.getAndRemoveAll(keys, options)))
            .collect(Collectors.toMap(CacheEntry::key, ce -> ce)));
   }

   @Override
   public long estimateSize(CacheOptions options) {
      return dispatcher.await(remoteCache.estimateSize(options));
   }

   @Override
   public void clear(CacheOptions options) {
      dispatcher.await(remoteCache.clear(options));
   }

   @Override
   public <R> SyncQuery<K, V, R> query(String query, CacheOptions options) {
      return new HotRodSyncQuery<>();
   }

   @Override
   public AutoCloseable listen(SyncCacheEntryListener<K, V> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Set<CacheEntryProcessorResult<K, T>> process(Set<K> keys, SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      Flow.Publisher<CacheEntryProcessorResult<K, T>> flowPublisher = remoteCache.process(keys,
            // TODO: do we worry about the sync processor here? Guessing we need to offload it to another thread ?
            new SyncToAsyncEntryProcessor<>(processor), options);
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(flowPublisher))
            .collect(Collectors.toSet()));
   }

   @Override
   public <T> Set<CacheEntryProcessorResult<K, T>> processAll(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      Flow.Publisher<CacheEntryProcessorResult<K, T>> flowPublisher = remoteCache.processAll(
            // TODO: do we worry about the sync processor here? Guessing we need to offload it to another thread ?
            new SyncToAsyncEntryProcessor<>(processor), options);
      return blockingGet(Flowable.fromPublisher(FlowAdapters.toPublisher(flowPublisher))
            .collect(Collectors.toSet()));
   }

   @Override
   public SyncStreamingCache<K> streaming() {
      return new HotRodSyncStreamingCache<>();
   }

   private record SyncToAsyncEntryProcessor<K, V, T>(SyncCacheEntryProcessor<K, V, T> processor) implements AsyncCacheEntryProcessor<K, V, T> {

      @Override
      public Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Flow.Publisher<MutableCacheEntry<K, V>> entries, CacheEntryProcessorContext context) {
         Flowable<CacheEntryProcessorResult<K, T>> flowable = Flowable.fromPublisher(FlowAdapters.toPublisher(entries))
               .map(e -> {
                  try {
                     return CacheEntryProcessorResult.onResult(e.key(), processor.process(e, context));
                  } catch (Throwable t) {
                     return CacheEntryProcessorResult.onError(e.key(), t);
                  }
               });
         return FlowAdapters.toFlowPublisher(flowable);
      }
   }
}
