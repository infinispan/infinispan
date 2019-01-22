package org.infinispan.api.collections.reactive;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.search.reactive.Searchable;
import org.reactivestreams.Publisher;

/**
 * @param <K>
 * @param <V>
 */
public interface ReactiveCache<K, V> {

   CompletionStage<V> get(K key);

   CompletionStage<Void> put(K key, V value);

   CompletionStage<V> getAndPut(K key, V value);

   CompletionStage<Void> remove(K key);

   CompletionStage<Boolean> remove(K key, V value);

   CompletionStage<V> getAndRemove(K key);

   CompletionStage<Void> putMany(Publisher<Map.Entry<K, V>> pairs);

   Publisher<K> getKeys();

   Publisher<V> getValues();

   CompletionStage<Long> size();

   Searchable<V> asSearchable();

   CompletionStage<Void> clear();
}
