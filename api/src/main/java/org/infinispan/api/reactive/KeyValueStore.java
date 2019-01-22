package org.infinispan.api.reactive;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.Experimental;
import org.infinispan.api.reactive.listener.KeyValueStoreListener;
import org.infinispan.api.reactive.query.QueryRequest;
import org.reactivestreams.Publisher;

/**
 * A Reactive Key Value Store provides a highly concurrent and distributed data structure, non blocking and using
 * reactive streams.
 * <p>
 *
 * </p>
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://www.infinispan.org">Infinispan documentation</a>
 * @since 10.0
 */
@Experimental
public interface KeyValueStore<K, V> {

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   CompletionStage<V> get(K key);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return Void
    */
   CompletionStage<Boolean> insert(K key, V value);

   /**
    * Save the key/value. If the key exists will replace the value
    *
    * @param key
    * @param value
    * @return Void
    */
   CompletionStage<Void> save(K key, V value);

   /**
    * Delete the key
    *
    * @param key
    * @return Void
    */
   CompletionStage<Void> delete(K key);

   /**
    * Retrieve all keys
    *
    * @return Publisher
    */
   Publisher<K> keys();

   /**
    * Retrieve all entries
    *
    * @return
    */
   Publisher<? extends Map.Entry<K, V>> entries();

   /**
    * Save many from a Publisher
    *
    * @param pairs
    * @return Void
    */
   Publisher<WriteResult<K>> saveMany(Publisher<Map.Entry<K, V>> pairs);

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   CompletionStage<Long> estimateSize();

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work
    *
    * @return Void
    */
   CompletionStage<Void> clear();

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param ickleQuery query String
    * @return Publisher reactive streams
    */
   Publisher<KeyValueEntry<K, V>> find(String ickleQuery);

   /**
    * Find by QueryRequest.
    *
    * @param queryRequest
    * @return
    */
   Publisher<KeyValueEntry<K, V>> find(QueryRequest queryRequest);

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param ickleQuery query String
    * @return Publisher reactive streams
    */
   Publisher<KeyValueEntry<K, V>> findContinuously(String ickleQuery);

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param queryRequest
    * @return Publisher reactive streams
    */
   <T> Publisher<KeyValueEntry<K, T>> findContinuously(QueryRequest queryRequest);

   /**
    * Listens to the {@link KeyValueStoreListener}
    *
    * @param listener
    * @return Publisher reactive streams
    */
   Publisher<KeyValueEntry<K, V>> listen(KeyValueStoreListener listener);

}
