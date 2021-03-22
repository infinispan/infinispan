package org.infinispan.api.sync;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.common.Flag;
import org.infinispan.api.common.events.ListenerHandle;
import org.infinispan.api.common.tasks.EntryConsumerTask;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.events.cache.SyncCacheContinuousQueryListener;
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
    * Returns a new instance of this cache with the supplied {@link Flag}s
    *
    * @param flags
    * @return
    */
   SyncCache<K, V> withFlags(Set<Flag> flags);

   /**
    * Returns a new instance of this cache with the supplied {@link Flag}s
    *
    * @param flags
    * @return
    */
   SyncCache<K, V> withFlags(Flag... flags);

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   V get(K key);

   /**
    * Insert the key/value pair. Returns the previous value if present.
    *
    * @param key
    * @param value
    * @return Void
    */
   V put(K key, V value);

   /**
    * Similar to {@link #put(Object, Object)} but does not return the previous value.
    */
   void set(K key, V value);

   /**
    * Save the key/value.
    *
    * @param key
    * @param value
    * @return true if the entry was put
    */
   boolean putIfAbsent(K key, V value);

   /**
    * Delete the key
    *
    * @param key
    * @return true if the entry was removed
    */
   boolean remove(K key);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   V getAndRemove(K key);

   /**
    * Retrieve all keys
    *
    * @return
    */
   CloseableIterable<K> keys();

   /**
    * Retrieve all entries
    *
    * @return
    */
   CloseableIterable<? extends CacheEntry<K, V>> entries();

   /**
    * Puts all entries
    *
    * @param entries
    * @return Void
    */
   void putAll(Map<K, V> entries);

   /**
    * Retrieves all entries for the supplied keys
    *
    * @param keys
    * @return
    */
   Map<K, V> getAll(Set<K> keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Set<K> removeAll(Set<K> keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Map<K, V> getAndRemoveAll(Set<K> keys);

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   long estimateSize();

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work.
    */
   void clear();

   /**
    * Executes the query and returns an {@link Iterable}
    *
    * @param query query String
    * @return
    */
   <R> Iterable<SyncQueryResult<R>> find(String query);

   /**
    * Find by query
    *
    * @param query
    * @return
    */
   <R> SyncQuery<R> query(String query);

   /**
    * Continuously listen on query
    *
    * @param query
    * @param listener
    * @param <R>
    * @return a {@link ListenerHandle} which can be used to remove the listener
    */
   <R> ListenerHandle<SyncCacheContinuousQueryListener<K, V>> listen(SyncQuery<R> query, SyncCacheContinuousQueryListener<K, V> listener);

   /**
    * Listens to the {@link SyncCacheEntryListener}
    *
    * @param listener
    * @return a {@link ListenerHandle} which can be used to remove the listener
    */
   ListenerHandle<SyncCacheEntryListener<K, V>> listen(SyncCacheEntryListener<K, V> listener);

   /**
    * @param key
    * @param remappingFunction
    * @return
    */
   V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

   /**
    * @param key
    * @param mappingFunction
    * @return
    */
   V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

   /**
    * Process all entries using the supplied task
    * @param task
    */
   void process(EntryConsumerTask<K, V> task);

   /**
    *
    * @return
    */
   SyncStreamingCache<K> streaming();
}
