package org.infinispan.api.async;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.tasks.EntryConsumerTask;
import org.infinispan.api.configuration.CacheConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncCache<K, V> {
   /**
    * The name of the cache.
    *
    * @return
    */
   String name();

   /**
    * The configuration for this cache.
    *
    * @return
    */
   CompletionStage<CacheConfiguration> configuration();

   /**
    * Return the container of this cache
    * @return
    */
   AsyncContainer container();

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
   CompletionStage<Boolean> putIfAbsent(K key, V value);

   /**
    * @param key
    * @param value
    * @return Void
    */
   CompletionStage<V> put(K key, V value);

   /**
    * @param key
    * @param value
    * @return
    */
   CompletionStage<Void> set(K key, V value);

   /**
    * Delete the key
    *
    * @param key
    * @return whether the entry was removed.
    */
   CompletionStage<Boolean> remove(K key);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   CompletionStage<V> getAndRemove(K key);

   /**
    * Retrieve all keys
    *
    * @return
    */
   Flow.Publisher<K> keys();

   /**
    * Retrieve all entries
    *
    * @return
    */
   Flow.Publisher<? extends CacheEntry<K, V>> entries();

   /**
    * @param entries
    * @return Void
    */
   CompletionStage<Void> putAll(Map<K, V> entries);

   /**
    * Retrieves the entries for the specified keys.
    *
    * @param keys
    * @return
    */
   Flow.Publisher<? extends CacheEntry<K, V>> getMany(Set<K> keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Flow.Publisher<K> removeAll(Set<K> keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Flow.Publisher<? extends CacheEntry<K, V>> getAndRemoveAll(Set<K> keys);

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
    * Executes the query and returns an iterable with the entries
    *
    * @param query query String
    * @return Publisher reactive streams
    */
   <R> CompletionStage<AsyncQueryResult<R>> find(String query);

   /**
    * Executes the query and returns a {@link java.util.concurrent.Flow.Publisher} with the results
    *
    * @param query query String
    * @return a {@link Flow.Publisher} which produces {@link CacheContinuousQueryEvent} items.
    */
   <R> Flow.Publisher<CacheContinuousQueryEvent<K, R>> findContinuously(String query);

   /**
    * Register a cache listener with default {@link CacheListenerOptions}
    * @param types
    * @return
    */
   default Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheEntryEventType... types) {
      return listen(new CacheListenerOptions(), types);
   }

   /**
    * Register a cache listener with the supplied {@link CacheListenerOptions}
    * @param options
    * @param types one or more {@link CacheEntryEventType}
    * @return
    */
   Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types);

   /**
    * Process all entries using the supplied task
    * @param task
    * @return
    */
   CompletionStage<Void> process(EntryConsumerTask<K, V> task);

   AsyncStreamingCache<K> streaming();
}
