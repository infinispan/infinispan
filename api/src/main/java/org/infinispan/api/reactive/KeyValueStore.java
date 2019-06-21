package org.infinispan.api.reactive;

import java.util.Map;
import java.util.concurrent.CompletionStage;

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

   CompletionStage<Void> create(K key, V value);

   CompletionStage<Void> save(K id, V value);

   CompletionStage<V> getAndSave(K key, V value);

   CompletionStage<Void> delete(K key);

   CompletionStage<V> getAndDelete(K key);

   Publisher<K> keys();

   Publisher<Map.Entry<K, V>> entries();

   CompletionStage<Void> saveMany(Publisher<Map.Entry<K, V>> pairs);

   CompletionStage<Long> estimateSize();

   CompletionStage<Void> clear();

   QueryPublisher find(String ickleQuery);

   QueryPublisher find(QueryRequest queryRequest);

   ContinuousQueryPublisher findContinuously(String ickleQuery);

   ContinuousQueryPublisher findContinuously(QueryRequest queryRequest);

}
