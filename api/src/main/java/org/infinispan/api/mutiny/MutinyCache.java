package org.infinispan.api.mutiny;

import java.util.Map;
import java.util.Set;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.configuration.CacheConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * A Reactive Cache provides a highly concurrent and distributed data structure, non blocking and using reactive
 * streams.
 *
 * @since 14.0
 */
@Experimental
public interface MutinyCache<K, V> {
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
   Uni<CacheConfiguration> configuration();

   /**
    * Return the container of this cache
    *
    * @return
    */
   MutinyContainer container();

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   default Uni<V> get(K key) {
      return get(key, CacheOptions.DEFAULT);
   }

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @param options
    * @return the value
    */
   Uni<V> get(K key, CacheOptions options);

   /**
    * @param key
    * @return
    */
   default Uni<CacheEntry<K, V>> getEntry(K key) {
      return getEntry(key, CacheOptions.DEFAULT);
   }

   /**
    * @param key
    * @param options
    * @return
    */
   Uni<CacheEntry<K, V>> getEntry(K key, CacheOptions options);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return the previous value if present
    */
   default Uni<V> putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return the previous value if present
    */
   Uni<V> putIfAbsent(K key, V value, CacheWriteOptions options);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return
    */
   default Uni<Boolean> setIfAbsent(K key, V value) {
      return setIfAbsent(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   Uni<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options);

   /**
    * Save the key/value. If the key exists will replace the value
    *
    * @param key
    * @param value
    * @return
    */
   default Uni<V> put(K key, V value) {
      return put(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   Uni<V> put(K key, V value, CacheWriteOptions options);

   /**
    * @param key
    * @param value
    * @return
    */
   default Uni<Void> set(K key, V value) {
      return set(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   Uni<Void> set(K key, V value, CacheWriteOptions options);

   /**
    * Delete the key
    *
    * @param key
    * @return true if the key existed and was removed, false if the key did not exist.
    */
   default Uni<Boolean> remove(K key) {
      return remove(key, CacheOptions.DEFAULT);
   }

   /**
    * Delete the key
    *
    * @param key
    * @param options
    * @return true if the key existed and was removed, false if the key did not exist.
    */
   Uni<Boolean> remove(K key, CacheOptions options);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   default Uni<V> getAndRemove(K key) {
      return getAndRemove(key, CacheOptions.DEFAULT);
   }

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @param options
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   Uni<V> getAndRemove(K key, CacheOptions options);

   /**
    * Retrieve all keys
    *
    * @return @{@link Multi} which produces keys as items.
    */
   default Multi<K> keys() {
      return keys(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all keys
    *
    * @return @{@link Multi} which produces keys as items.
    */
   Multi<K> keys(CacheOptions options);

   /**
    * Retrieve all entries
    *
    * @return
    */
   default Multi<CacheEntry<K, V>> entries() {
      return entries(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all entries
    *
    * @param options
    * @return
    */
   Multi<CacheEntry<K, V>> entries(CacheOptions options);

   /**
    * Retrieve all the entries for the specified keys.
    *
    * @param keys
    * @return
    */
   default Multi<CacheEntry<K, V>> getAll(Set<K> keys) {
      return getAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all the entries for the specified keys.
    *
    * @param keys
    * @param options
    * @return
    */
   Multi<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options);

   /**
    * @param keys
    * @return
    */
   default Multi<CacheEntry<K, V>> getAll(K... keys) {
      return getAll(CacheOptions.DEFAULT, keys);
   }

   /**
    * @param keys
    * @param options
    * @return
    */
   Multi<CacheEntry<K, V>> getAll(CacheOptions options, K... keys);

   /**
    * Put multiple entries from a {@link Multi}
    *
    * @param pairs
    * @return Void
    */
   default Uni<Void> putAll(Multi<CacheEntry<K, V>> pairs) {
      return putAll(pairs, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param pairs
    * @param options
    * @return
    */
   Uni<Void> putAll(Multi<CacheEntry<K, V>> pairs, CacheWriteOptions options);

   /**
    * @param map
    * @param options
    * @return
    */
   Uni<Void> putAll(Map<K, V> map, CacheWriteOptions options);

   default Uni<Void> putAll(Map<K, V> map) {
      return putAll(map, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @return
    */
   default Uni<Boolean> replace(K key, V value, CacheEntryVersion version) {
      return replace(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   Uni<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   /**
    * @param key
    * @param value
    * @param version
    * @return
    */
   default Uni<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version) {
      return getOrReplaceEntry(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @param version
    * @return
    */
   Uni<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Multi<K> removeAll(Set<K> keys) {
      return removeAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Multi<K> removeAll(Set<K> keys, CacheOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Multi<K> removeAll(Multi<K> keys) {
      return removeAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Multi<K> removeAll(Multi<K> keys, CacheOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Multi<CacheEntry<K, V>> getAndRemoveAll(Multi<K> keys) {
      return getAndRemoveAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Multi<CacheEntry<K, V>> getAndRemoveAll(Multi<K> keys, CacheOptions options);

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   default Uni<Long> estimateSize() {
      return estimateSize(CacheOptions.DEFAULT);
   }

   /**
    * Estimate the size of the store
    *
    * @param options
    * @return Long, estimated size
    */
   Uni<Long> estimateSize(CacheOptions options);

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work
    *
    * @return Void
    */
   default Uni<Void> clear() {
      return clear(CacheOptions.DEFAULT);
   }

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work
    *
    * @param options
    * @return Void
    */
   Uni<Void> clear(CacheOptions options);

   /**
    * Find by QueryRequest.
    *
    * @param <R>
    * @param query
    * @return
    */
   default <R> MutinyQuery<K, V, R> query(String query) {
      return query(query, CacheOptions.DEFAULT);
   }

   /**
    * Find by QueryRequest.
    *
    * @param <R>
    * @param query
    * @param options
    * @return
    */
   <R> MutinyQuery<K, V, R> query(String query, CacheOptions options);

   /**
    * Listens to the events
    *
    * @param types
    * @return a {@link Multi} which produces {@link CacheEntryEvent} items.
    */
   default Multi<CacheEntryEvent<K, V>> listen(CacheEntryEventType... types) {
      return listen(new CacheListenerOptions(), types);
   }

   /**
    * Listens to the events
    *
    * @param options
    * @param types
    * @return a {@link Multi} which produces {@link CacheEntryEvent} items.
    */
   Multi<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types);

   /**
    * Process the specified entries using the supplied processor
    *
    * @param keys
    * @param processor
    */
   default <T> Multi<CacheEntryProcessorResult<K, T>> process(Set<K> keys, MutinyCacheEntryProcessor<K, V, T> processor) {
      return process(keys, processor, CacheOptions.DEFAULT);
   }

   /**
    * Process the specified entries using the supplied processor
    *
    * @param keys
    * @param processor
    * @param options
    */
   <T> Multi<CacheEntryProcessorResult<K, T>> process(Set<K> keys, MutinyCacheEntryProcessor<K, V, T> processor, CacheOptions options);

   /**
    * Process all entries using the supplied task
    *
    * @param task
    */
   default <T> Multi<CacheEntryProcessorResult<K, T>> processAll(CacheProcessor task) {
      return processAll(task, CacheOptions.DEFAULT);
   }

   /**
    * Process all entries using the supplied task
    *
    * @param task
    * @param options
    */
   <T> Multi<CacheEntryProcessorResult<K, T>> processAll(CacheProcessor task, CacheOptions options);

   /**
    * @return
    */
   MutinyStreamingCache<K> streaming();
}
