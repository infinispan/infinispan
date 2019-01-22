package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.collections.reactive.KeyValueStore;
import org.infinispan.api.search.reactive.SearchableStore;
import org.infinispan.client.hotrod.RemoteCache;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
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
      return CompletableFuture.runAsync(() -> {
         Flowable<KeyValueEntry<K, V>> entryFlowable = Flowable.fromPublisher(pairs);
         entryFlowable.subscribe(e -> cache.putAsync(e.getKey(), e.getValue()),
               err -> new RuntimeException("Error in put many")
         );
      });
   }

   @Override
   public Publisher<KeyValueEntry<K, V>> entries() {
      return Flowable.fromIterable((Iterable<? extends KeyValueEntry<K, V>>) cache.values());
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
   public SearchableStore<V> asSearchable() {
      return new SearchableKeyValueStoreImpl<K, V>(cache, cacheReturnValues);
   }
}
