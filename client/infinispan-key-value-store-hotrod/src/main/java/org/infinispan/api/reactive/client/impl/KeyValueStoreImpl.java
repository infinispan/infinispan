package org.infinispan.api.reactive.client.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.client.listener.ClientKeyValueStoreListener;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.WriteResult;
import org.infinispan.api.reactive.client.impl.listener.ClientListenerImpl;
import org.infinispan.api.reactive.listener.KeyValueStoreListener;
import org.infinispan.api.reactive.query.QueryRequest;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.UnicastProcessor;

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
   public CompletionStage<Boolean> insert(K key, V value) {
      return cacheReturnValues.putIfAbsentAsync(key, value).thenApply(v -> v == null);
   }

   @Override
   public CompletionStage<Void> save(K key, V value) {
      // We don't return the value here
      return cache.putAsync(key, value).thenApply(v -> null);
   }

   @Override
   public CompletionStage<Void> delete(K key) {
      return cache.removeAsync(key).thenApply(v -> null);
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
   public Publisher<? extends Map.Entry<K, V>> entries() {
      UnicastProcessor<Map.Entry<K, V>> processor = UnicastProcessor.create();
      cache.entrySet().iterator().forEachRemaining(e -> {
         processor.onNext(e);
      });
      return processor;
   }

   @Override
   public Publisher<WriteResult<K>> saveMany(Publisher<Map.Entry<K, V>> pairs) {
      UnicastProcessor<WriteResult<K>> unicastProcessor = UnicastProcessor.create();

      Flowable<Map.Entry<K, V>> entryFlowable = Flowable.fromPublisher(pairs);

      entryFlowable.subscribe(e -> {
         cache.putAsync(e.getKey(), e.getValue())
               .whenComplete((r, ex) -> unicastProcessor.onNext(new WriteResult<>(e.getKey(), ex)));
      });

      return unicastProcessor;
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      return cache.sizeAsync();
   }

   @Override
   public CompletionStage<Void> clear() {
      return cache.clearAsync();
   }

   @Override
   public Publisher<KeyValueEntry<K, V>> find(String ickleQuery) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public Publisher<KeyValueEntry<K, V>> find(QueryRequest queryRequest) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery());
      query.setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      query.maxResults(queryRequest.limit());
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService());
   }

   @Override
   public Publisher<KeyValueEntry<K, V>> findContinuously(String ickleQuery) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new ContinuousQueryPublisherImpl(query, continuousQuery, true, true, true);
   }

   @Override
   public <T> Publisher<KeyValueEntry<K, T>> findContinuously(QueryRequest queryRequest) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery())
            .setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      return new ContinuousQueryPublisherImpl(query, continuousQuery, queryRequest.isCreated(), queryRequest.isUpdated(), queryRequest.isDeleted());
   }

   @Override
   public Publisher<KeyValueEntry<K, V>> listen(KeyValueStoreListener listener) {
      // TODO CHECK CAST. Now there is a single class that implements
      return new ClientListenerImpl(cache, (ClientKeyValueStoreListener) listener);
   }
}
