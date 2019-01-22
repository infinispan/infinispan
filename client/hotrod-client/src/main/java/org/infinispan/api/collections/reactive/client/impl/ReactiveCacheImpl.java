package org.infinispan.api.collections.reactive.client.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.collections.reactive.Query;
import org.infinispan.api.collections.reactive.ReactiveCache;
import org.infinispan.api.search.reactive.Searchable;
import org.infinispan.client.hotrod.RemoteCache;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @since 10.0
 */
public class ReactiveCacheImpl<K, V> implements ReactiveCache<K, V>, Searchable<V> {
   protected final RemoteCache<K, V> cache;
   protected RemoteCache<K, V> cacheReturnValues;

   public ReactiveCacheImpl(RemoteCache<K, V> cache, RemoteCache<K, V> cacheReturnValues) {
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
   public CompletionStage<Void> remove(K key) {
      return cache.removeAsync(key).thenApply(r -> null);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, V value) {
      return cache.removeAsync(key, value);
   }

   @Override
   public CompletionStage<V> getAndPut(K key, V value) {
      return cacheReturnValues.putAsync(key, value);
   }

   @Override
   public CompletionStage<Void> putMany(Publisher<Map.Entry<K, V>> pairs) {
      return CompletableFuture.runAsync(() -> {
         Flowable<Map.Entry<K, V>> entryFlowable = Flowable.fromPublisher(pairs);
         entryFlowable.subscribe(e -> cache.putAsync(e.getKey(), e.getValue()),
               err -> new RuntimeException("Error in put many")
         );
      });
   }

   @Override
   public Publisher<K> getKeys() {
      return Flowable.fromIterable(cache.keySet());
   }

   @Override
   public Publisher<V> getValues() {
      return Flowable.fromIterable(cache.values());
   }

   @Override
   public CompletionStage<Long> size() {
      return CompletableFuture.supplyAsync(() -> Long.valueOf(cache.size()));
   }

   @Override
   public Searchable<V> asSearchable() {
      return this;
   }

   @Override
   public CompletionStage<Void> clear() {
      return cache.clearAsync();
   }

   @Override
   public Publisher<V> find(String ickleQuery) {
      return null;
   }

   @Override
   public Publisher<V> find(Query query) {
      return null;
   }

   @Override
   public Publisher<V> findContinuous(String ickleQuery) {
      return null;
   }

   @Override
   public Publisher<V> findContinuous(Query query) {
      return null;
   }
}
