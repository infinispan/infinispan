package org.infinispan.api.collections.reactive;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.search.reactive.ReactiveContinuousQuery;
import org.infinispan.api.search.reactive.ReactiveQuery;
import org.reactivestreams.Publisher;

/**
 * A Reactive Key Value Store provides a highly concurrent and distributed data structure, non blocking and using
 * reactive streams.
 * <p>
 * TODO: Add more javadoc and examples
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://www.infinispan.org">Infinispan documentation</a>
 * @since 10.0
 */
public interface KeyValueStore<K, V> {

   CompletionStage<V> get(K key);

   CompletionStage<Void> put(K key, V value);

   CompletionStage<V> getAndPut(K key, V value);

   CompletionStage<Void> remove(K key);

   CompletionStage<V> getAndRemove(K key);

   CompletionStage<Void> putMany(Publisher<KeyValueEntry<K, V>> pairs);

   Publisher<KeyValueEntry<K, V>> entries();

   CompletionStage<Long> estimateSize();

   CompletionStage<Void> clear();

   ReactiveQuery find(String ickleQuery);

   ReactiveContinuousQuery findContinuously(String ickleQuery);
}
