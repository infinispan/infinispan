package org.infinispan.api.async;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
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
    *
    * @return
    */
   AsyncContainer container();

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   default CompletionStage<V> get(K key) {
      return get(key, CacheOptions.DEFAULT);
   }

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @param options
    * @return the value
    */
   default CompletionStage<V> get(K key, CacheOptions options) {
      return getEntry(key, options)
            .thenApply(ce -> ce != null ? ce.value() : null);
   }

   /**
    * Get the entry of the Key if such exists
    *
    * @param key
    * @return the entry
    */
   default CompletionStage<CacheEntry<K, V>> getEntry(K key) {
      return getEntry(key, CacheOptions.DEFAULT);
   }

   /**
    * Get the entry of the Key if such exists
    *
    * @param key
    * @param options
    * @return the entry
    */
   CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return the previous value if present
    */
   default CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value) {
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
   CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return
    */
   default CompletionStage<Boolean> setIfAbsent(K key, V value) {
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
   default CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return putIfAbsent(key, value, options)
            .thenApply(Objects::isNull);
   }

   /**
    * @param key
    * @param value
    * @return Void
    */
   default CompletionStage<CacheEntry<K, V>> put(K key, V value) {
      return put(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return Void
    */
   CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options);

   /**
    * @param key
    * @param value
    * @return
    */
   default CompletionStage<Void> set(K key, V value) {
      return set(key, value, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   default CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      return put(key, value, options)
            .thenApply(__ -> null);
   }

   /**
    * @param key
    * @param value
    * @return
    */
   default CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version) {
      return replace(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @return
    */
   default CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return getOrReplaceEntry(key, value, version, options)
            .thenApply(ce -> ce != null && version.equals(ce.metadata().version()));
   }

   /**
    * @param key
    * @param value
    * @param version
    * @return
    */
   default CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version) {
      return getOrReplaceEntry(key, value, version, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param value
    * @param options
    * @param version
    * @return
    */
   CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   /**
    * Delete the key
    *
    * @param key
    * @return whether the entry was removed.
    */
   default CompletionStage<Boolean> remove(K key) {
      return remove(key, CacheOptions.DEFAULT);
   }

   /**
    * Delete the key
    *
    * @param key
    * @param options
    * @return whether the entry was removed.
    */
   CompletionStage<Boolean> remove(K key, CacheOptions options);

   /**
    * Delete the key only if the version matches
    *
    * @param key
    * @param version
    * @return whether the entry was removed.
    */
   default CompletionStage<Boolean> remove(K key, CacheEntryVersion version) {
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
   CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   default CompletionStage<CacheEntry<K, V>> getAndRemove(K key) {
      return getAndRemove(key, CacheOptions.DEFAULT);
   }

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @param options
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options);

   /**
    * Retrieve all keys
    *
    * @return
    */
   default Flow.Publisher<K> keys() {
      return keys(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all keys
    *
    * @param options
    * @return
    */
   Flow.Publisher<K> keys(CacheOptions options);

   /**
    * Retrieve all entries
    *
    * @return
    */
   default Flow.Publisher<CacheEntry<K, V>> entries() {
      return entries(CacheOptions.DEFAULT);
   }

   /**
    * Retrieve all entries
    *
    * @param options
    * @return
    */
   Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options);

   /**
    * @param entries
    * @return Void
    */
   default CompletionStage<Void> putAll(Map<K, V> entries) {
      return putAll(entries, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param entries
    * @param options
    * @return
    */
   CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options);

   /**
    * @param entries
    * @return Void
    */
   default CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries) {
      return putAll(entries, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param entries
    * @param options
    * @return
    */
   CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options);

   /**
    * Retrieves the entries for the specified keys.
    *
    * @param keys
    * @return
    */
   default Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys) {
      return getAll(keys, CacheOptions.DEFAULT);
   }

   /**
    * Retrieves the entries for the specified keys.
    *
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options);

   /**
    * Retrieves the entries for the specified keys.
    *
    * @param keys
    * @return
    */
   default Flow.Publisher<CacheEntry<K, V>> getAll(K... keys) {
      return getAll(CacheOptions.DEFAULT, keys);
   }

   /**
    * Retrieves the entries for the specified keys.
    *
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K... keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Flow.Publisher<K> removeAll(Set<K> keys) {
      return removeAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options);

   /**
    * @param keys
    * @return Void
    */
   default Flow.Publisher<K> removeAll(Flow.Publisher<K> keys) {
      return removeAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys) {
      return getAndRemoveAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   default Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys) {
      return getAndRemoveAll(keys, CacheWriteOptions.DEFAULT);
   }

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @param options
    * @return
    */
   Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options);

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   default CompletionStage<Long> estimateSize() {
      return estimateSize(CacheOptions.DEFAULT);
   }

   /**
    * Estimate the size of the store
    *
    * @param options
    * @return Long, estimated size
    */
   CompletionStage<Long> estimateSize(CacheOptions options);

   /**
    * Clear the cache. If a concurrent operation puts data in the cache the clear might work properly
    *
    * @return Void
    */
   default CompletionStage<Void> clear() {
      return clear(CacheOptions.DEFAULT);
   }

   /**
    * Clear the cache. If a concurrent operation puts data in the cache the clear might not properly work
    *
    * @param options
    * @return Void
    */
   CompletionStage<Void> clear(CacheOptions options);

   /**
    * @param <R>
    * @param query query String
    * @return
    */
   default <R> AsyncQuery<K, V, R> query(String query) {
      return query(query, CacheOptions.DEFAULT);
   }

   /**
    * Executes the query and returns an iterable with the entries
    *
    * @param <R>
    * @param query   query String
    * @param options
    * @return
    */
   <R> AsyncQuery<K, V, R> query(String query, CacheOptions options);

   /**
    * Register a cache listener with default {@link CacheListenerOptions}
    *
    * @param types
    * @return
    */
   default Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheEntryEventType... types) {
      return listen(new CacheListenerOptions(), types);
   }

   /**
    * Register a cache listener with the supplied {@link CacheListenerOptions}
    *
    * @param options
    * @param types   one or more {@link CacheEntryEventType}
    * @return
    */
   Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types);

   /**
    * Process entries using the supplied task
    *
    * @param keys
    * @param processor
    * @return
    */
   default <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> processor) {
      return process(keys, processor, CacheOptions.DEFAULT);
   }

   /**
    * Process entries using the supplied task
    *
    * @param keys
    * @param task
    * @param options
    * @return
    */
   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> task, CacheOptions options);

   /**
    * Execute a {@link CacheProcessor} on a cache
    *
    * @param <T>
    * @param processor
    * @return
    */
   default <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor) {
      return processAll(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Execute a {@link CacheProcessor} on a cache
    *
    * @param <T>
    * @param processor
    * @param options
    * @return
    */
   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   /**
    * @return
    */
   AsyncStreamingCache<K> streaming();
}
