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

   /**
    * Get key value
    *
    * @param key
    * @return
    */
   CompletionStage<V> get(K key);

   /**
    * @param key
    * @param value
    * @return
    */
   CompletionStage<Void> put(K key, V value);

   /**
    * @param key
    * @param value
    * @return
    */
   CompletionStage<V> getAndPut(K key, V value);

   /**
    * @param key
    * @return
    */
   CompletionStage<Void> remove(K key);

   /**
    * @param key
    * @return
    */
   CompletionStage<V> getAndRemove(K key);


   //Publisher<K> keys();

   //Publisher<KeyValueEntry> entries();

   /**
    * @param pairs
    * @return
    */
   CompletionStage<Void> putMany(Publisher<Map.Entry<K, V>> pairs);

   /**
    * Estimation of the size of the store
    *
    * @return {@link CompletionStage<Long>}
    */
   CompletionStage<Long> estimateSize();

   /**
    * Clear the key-value store
    *
    * Concurrent operations, use it carefully*
    *
    * @return {@link CompletionStage<Void>}
    */
   CompletionStage<Void> clear();

   /**
    * Provides the entry point to create queries in a reactive way.
    *
    * @return query publisher {@link QueryPublisher}
    */
   QueryPublisher find(String ickleQuery);

   QueryPublisher find(Query query);

   QueryRequestBuilder(string).queryParam(...).limit().skip().build();

   /**
    * Provides the entry point to create a continuous query in a reactive way.
    *
    * @return continuous query publisher {@link ContinuousQueryPublisher}
    */
   ContinuousQueryPublisher<K, V> findContinuously(String ickleQuery);

   ContinuousQueryPublisher<K, V> find(ContinuousQuery query);

}
