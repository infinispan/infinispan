package org.infinispan.api.collections.reactive.client.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.exception.InfinispanException;
import org.infinispan.api.reactive.ContinuousQueryPublisher;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.QueryPublisher;
import org.infinispan.api.reactive.QueryRequest;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;

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
   public CompletionStage<Void> create(K key, V value) {
      // We don't return the value here
      return cache.putIfAbsentAsync(key, value).thenApply(v -> null);
   }

   @Override
   public CompletionStage<Void> save(K key, V value) {
      // We don't return the value here
      return cache.putAsync(key, value).thenApply(v -> null);
   }

   @Override
   public CompletionStage<V> getAndSave(K key, V value) {
      return cacheReturnValues.putAsync(key, value);
   }

   @Override
   public CompletionStage<Void> delete(K key) {
      return cache.removeAsync(key).thenApply(v -> null);
   }

   @Override
   public CompletionStage<V> getAndDelete(K key) {
      return cacheReturnValues.removeAsync(key);
   }

   @Override
   public Publisher<K> keys() {
      UnicastProcessor<K> processor = UnicastProcessor.create();
      cache.keySet().iterator().forEachRemaining(k -> {
         processor.onNext(k);
      });
      return processor;
   }

   @Override
   public Publisher<Map.Entry<K, V>> entries() {
      UnicastProcessor<Map.Entry<K, V>> processor = UnicastProcessor.create();
      cache.entrySet().iterator().forEachRemaining(e -> {
         processor.onNext(e);
      });
      return processor;
   }

   @Override
   public CompletionStage<Void> saveMany(Publisher<Map.Entry<K, V>> pairs) {
      return CompletableFuture.runAsync(() -> {
         Flowable<Map.Entry<K, V>> entryFlowable = Flowable.fromPublisher(pairs);
         entryFlowable.subscribe(e -> cache.putAsync(e.getKey(), e.getValue()),
               err -> new InfinispanException("KeyValueStore - Error in put many", err)
         );
      }, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      return CompletableFuture.supplyAsync(() -> Long.valueOf(cache.size()));
   }

   @Override
   public CompletionStage<Void> clear() {
      return cache.clearAsync();
   }

   @Override
   public QueryPublisher<V> find(String ickleQuery) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public QueryPublisher find(QueryRequest queryRequest) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery());
      query.setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      query.maxResults(queryRequest.limit());
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public ContinuousQueryPublisher<K, V> findContinuously(String ickleQuery) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new ContinuousQueryPublisherImpl(query, continuousQuery, true, true, true);
   }

   @Override
   public ContinuousQueryPublisher<K, V> findContinuously(QueryRequest queryRequest) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery())
            .setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      return new ContinuousQueryPublisherImpl(query, continuousQuery, queryRequest.isCreated(), queryRequest.isUpdated(), queryRequest.isDeleted());
   }
}
