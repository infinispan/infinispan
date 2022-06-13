package org.infinispan.api.sync;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.events.cache.SyncCacheEntryListener;

/**
 * @since 14.0
 **/
public interface SyncCache<K, V> {

   /**
    * Returns the name of this cache
    *
    * @return the name of the cache
    */
   String name();

   /**
    * Returns the configuration of this cache
    *
    * @return the cache configuration
    */
   CacheConfiguration configuration();

   /**
    * Return the container of this cache
    *
    * @return the cache container
    */
   SyncContainer container();

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   default V get(K key) {
      return get(key, CacheOptions.DEFAULT);
   }

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   default V get(K key, CacheOptions options) {
      CacheEntry<K, V> entry = getEntry(key, options);
      return entry == null ? null : entry.value();
   }

   /**
    * Get the entry of the Key if such exists
    *
    * @param key
    * @return the entry
    */
   default CacheEntry<K, V> getEntry(K key) {
      return getEntry(key, CacheOptions.DEFAULT);
   }

   /**
    * Get the entry of the Key if such exists
    *
    * @param key
    * @param options
    * @return the entry
    */
   CacheEntry<K, V> getEntry(K key, CacheOptions options);

   /**
    * Insert the key/value pair. Returns the previous value if present.
    *
    * @param key
    * @param value
    * @return Void
    */
   default CacheEntry<K, V> put(K key, V value) {
      return put(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return Void
    */
   CacheEntry<K, V> put(K key, V value, CacheWriteOptions options);

   /**
    * Similar to {@link #put(Object, Object)} but does not return the previous value.
    */
   default void set(K key, V value) {
      set(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   default void set(K key, V value, CacheWriteOptions options) {
      put(key, value, options);
   }

   /**
    * Save the key/value.
    *
    * @param key
    * @param value
    * @return the previous value if present
    */
   default CacheEntry<K, V> putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @param options
    * @return the previous value if present
    */
   CacheEntry<K, V> putIfAbsent(K key, V value, CacheWriteOptions options);

   /**
    * Save the key/value.
    *
    * @param key
    * @param value
    * @return true if the entry was set
    */
   default boolean setIfAbsent(K key, V value) {
      return setIfAbsent(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @param options
    * @return Void
    */
   default boolean setIfAbsent(K key, V value, CacheWriteOptions options) {
      CacheEntry<K, V> ce = putIfAbsent(key, value, options);
      return ce == null;
   }

   /**
    * @param key
    * @param value
    * @return
    */
   default boolean replace(K key, V value, CacheEntryVersion version) {
      return replace(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   default boolean replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      CacheEntry<K, V> ce = getOrReplaceEntry(key, value, version, options);
      return ce != null && version.equals(ce.metadata().version());
   }

   /**
    * @param key
    * @param value
    * @param version
    * @return
    */
   default CacheEntry<K, V> getOrReplaceEntry(K key, V value, CacheEntryVersion version) {
      return getOrReplaceEntry(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @param version
    * @return
    */
   CacheEntry<K, V> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   /**
    * Delete the key
    *
    * @param key
    * @return true if the entry was removed
    */
   default boolean remove(K key) {
      return remove(key, CacheOptions.DEFAULT);
   }

   /**
    * Delete the key
    *
    * @param key
    * @param options
    * @return true if the entry was removed
    */
   default boolean remove(K key, CacheOptions options) {
      CacheEntry<K, V> ce = getAndRemove(key, options);
      return ce != null;
   }

   /**
    * Delete the key only if the version matches
    *
    * @param key
    * @param version
    * @return whether the entry was removed.
    */
   default boolean remove(K key, CacheEntryVersion version) {
      return remove(key, version, CacheOptions.DEFAULT);
   }

   /**
    * Delete the key only if the version matches
    *
    * @param key
    * @param version
    * @param options
    * @return whether the entry was removed.
    */
   boolean remove(K key, CacheEntryVersion version, CacheOptions options);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   default CacheEntry<K, V> getAndRemove(K key) {
      return getAndRemove(key, CacheOptions.DEFAULT);
   }

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @param options
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   CacheEntry<K, V> getAndRemove(K key, CacheOptions options);

   /**
    * Retrieve all keys
    *
    * @return
    */
   default CloseableIterable<K> keys() {
      return keys(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all keys
    *
    * @param options
    * @return
    */
   CloseableIterable<K> keys(CacheOptions options);

   /**
    * Retrieve all entries
    *
    * @return
    */
   default CloseableIterable<CacheEntry<K, V>> entries() {
      return entries(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all entries
    *
    * @param options
    * @return
    */
   CloseableIterable<CacheEntry<K, V>> entries(CacheOptions options);

   /**
    * Puts all entries
    *
    * @param entries
    * @return Void
    */
   default void putAll(Map<K, V> entries) {
      putAll(entries, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param entries
    * @param options
    */
   void putAll(Map<K, V> entries, CacheWriteOptions options);

   /**
    * Retrieves all entries for the supplied keys
    *
    * @param keys
    * @return
    */
   default Map<K, V> getAll(Set<K> keys) {
      return getAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Retrieves all entries for the supplied keys
    *
    * @param keys
    * @param options
    * @return
    */
   Map<K, V> getAll(Set<K> keys, CacheOptions options);

   /**
    * Retrieves all entries for the supplied keys
    *
    * @param keys
    * @return
    */
   default Map<K, V> getAll(K... keys) {
      return getAll(CacheOptions.DEFAULT, keys);
   }

   /**
    * Retrieves all entries for the supplied keys
    *
    * @param keys
    * @param options
    * @return
    */
   Map<K, V> getAll(CacheOptions options, K... keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Set<K> removeAll(Set<K> keys) {
      return removeAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Set<K> removeAll(Set<K> keys, CacheWriteOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Map<K, CacheEntry<K, V>> getAndRemoveAll(Set<K> keys) {
      return getAndRemoveAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Map<K, CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      Map<K, CacheEntry<K, V>> map = new HashMap<>(keys.size());
      for (K key : keys) {
         map.put(key, getAndRemove(key));
      }
      return map;
   }

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   default long estimateSize() {
      return estimateSize(CacheOptions.DEFAULT);
   }

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   long estimateSize(CacheOptions options);

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work.
    */
   default void clear() {
      clear(CacheOptions.DEFAULT);
   }

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work.
    */
   void clear(CacheOptions options);

   /**
    * Find by query
    *
    * @param query
    * @return
    */
   default <R> SyncQuery<K, V, R> query(String query) {
      return query(query, CacheOptions.DEFAULT);
   }

   /**
    * Find by query
    *
    * @param query
    * @param options
    * @param <R>
    * @return
    */
   <R> SyncQuery<K, V, R> query(String query, CacheOptions options);

   /**
    * Listens to the {@link SyncCacheEntryListener}
    *
    * @param listener
    * @return A {@link AutoCloseable} that allows to remove the listener via {@link AutoCloseable#close()}.
    */
   AutoCloseable listen(SyncCacheEntryListener<K, V> listener);

   /**
    * Process entries using the supplied processor
    *
    * @param <T>
    * @param keys
    * @param processor
    */
   default <T> Set<CacheEntryProcessorResult<K, T>> process(Set<K> keys, SyncCacheEntryProcessor<K, V, T> processor) {
      return process(keys, processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Process entries using the supplied processor
    *
    * @param <T>
    * @param keys
    * @param processor
    * @param options
    */
   <T> Set<CacheEntryProcessorResult<K, T>> process(Set<K> keys, SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   /**
    * Process entries using the supplied processor
    *
    * @param <T>
    * @param processor
    */
   default <T> Set<CacheEntryProcessorResult<K, T>> processAll(SyncCacheEntryProcessor<K, V, T> processor) {
      return processAll(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Process entries using the supplied processor
    *
    * @param <T>
    * @param processor
    * @param options
    */
   <T> Set<CacheEntryProcessorResult<K, T>> processAll(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   /**
    * @return
    */
   SyncStreamingCache<K> streaming();
}
