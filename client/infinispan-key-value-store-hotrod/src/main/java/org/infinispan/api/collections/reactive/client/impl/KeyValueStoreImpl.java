package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.exception.InfinispanException;
import org.infinispan.api.reactive.ContinuousQueryPublisher;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.QueryPublisher;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Implements the {@link KeyValueStore} interface
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class KeyValueStoreImpl<K, V> implements KeyValueStore<K, V> {
   protected final RemoteCache<K, V> cache;
   protected RemoteCache<K, V> cacheReturnValues;

   public KeyValueStoreImpl(RemoteCache<K, V> cache, RemoteCache<K, V> cacheReturnValues) {
      this.cache = cache;
      this.cacheReturnValues = cacheReturnValues;
   }

   @Override
   public CompletionStage<V> get(K key) {
      return cache.getAsync(key);
   }

   @Override
   public CompletionStage<Void> put(K key, V value) {
      // We don't return the value here
      return cache.putAsync(key, value).thenApply(v -> null);
   }

   @Override
   public CompletionStage<V> getAndRemove(K key) {
      return cacheReturnValues.removeAsync(key);
   }

   @Override
   public CompletionStage<Void> putMany(Publisher<KeyValueEntry<K, V>> pairs) {
      return putManyAdapted(pairs);
   }

   private CompletionStage<Void> putManyAdapted(Publisher<KeyValueEntry<K, V>> pairs) {
      return CompletableFuture.runAsync(() -> {
         Flowable<KeyValueEntry<K, V>> entryFlowable = Flowable.fromPublisher(pairs);
         entryFlowable.subscribe(e -> cache.putAsync(e.key(), e.value()),
               err -> new InfinispanException("KeyValueStore - Error in put many", err)
         );
      }, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      return CompletableFuture.supplyAsync(() -> Long.valueOf(cache.size()));
   }

   @Override
   public CompletionStage<Void> remove(K key) {
      return cache.removeAsync(key).thenApply(r -> null);
   }

   @Override
   public CompletionStage<V> getAndPut(K key, V value) {
      return cacheReturnValues.putAsync(key, value);
   }

   @Override
   public CompletionStage<Void> clear() {
      return cache.clearAsync();
   }

   @Override
   public QueryPublisher<V> find(String ickleQuery) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      QueryPublisherImpl queryPublisher = new QueryPublisherImpl(queryFactory, cache.getRemoteCacheManager().getAsyncExecutorService());
      return queryPublisher;
   }

   @Override
   public ContinuousQueryPublisher<K, V> findContinuously(String ickleQuery) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      ContinuousQueryPublisherImpl continuousQueryPublisherImpl = new ContinuousQueryPublisherImpl(continuousQuery, queryFactory);
      return continuousQueryPublisherImpl;
   }
}
