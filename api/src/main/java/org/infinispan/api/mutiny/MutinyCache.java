package org.infinispan.api.mutiny;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.Flag;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.tasks.EntryConsumerTask;
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
    * @return
    */
   MutinyContainer container();

   /**
    * Returns a new instance of this cache with the supplied {@link Flag}s
    *
    * @param flags
    * @return
    */
   MutinyCache<K, V> withFlags(Set<Flag> flags);

   /**
    * Returns a new instance of this cache with the supplied {@link Flag}s
    *
    * @param flags
    * @return
    */
   MutinyCache<K, V> withFlags(Flag... flags);

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   Uni<V> get(K key);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return Void
    */
   Uni<Boolean> putIfAbsent(K key, V value);

   /**
    * Save the key/value. If the key exists will replace the value
    *
    * @param key
    * @param value
    * @return Void
    */
   Uni<V> put(K key, V value);

   /**
    * @param key
    * @param value
    * @return
    */
   Uni<Void> set(K key, V value);

   /**
    * Delete the key
    *
    * @param key
    * @return true if the key existed and was removed, false if the key did not exist.
    */
   Uni<Boolean> remove(K key);

   /**
    * Removes the key and returns its value if present.
    *
    * @param key
    * @return the value of the key before removal. Returns null if the key didn't exist.
    */
   Uni<V> getAndRemove(K key);

   /**
    * Retrieve all keys
    *
    * @return @{@link Multi} which produces keys as items.
    */
   Multi<K> keys();

   /**
    * Retrieve all entries
    *
    * @return
    */
   Multi<? extends CacheEntry<K, V>> entries();

   /**
    * Retrieve all the entries for the specified keys.
    *
    * @param keys
    * @return
    */
   Multi<? extends CacheEntry<K, V>> getMany(List<K> keys);

   Multi<? extends CacheEntry<K, V>> getMany(K... keys);

   /**
    * Put multiple entries from a {@link Multi}
    *
    * @param pairs
    * @return Void
    */
   Multi<MutinyWriteResult<K>> put(Multi<CacheEntry<K, V>> pairs);

   Multi<MutinyWriteResult<K>> put(List<CacheEntry<K, V>> pairs);

   Multi<MutinyWriteResult<K>> put(Map<K, V> map, CacheEntryMetadata metadata);

   Multi<MutinyWriteResult<K>> put(Map<K, V> map);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Multi<K> removeMany(Multi<K> keys);

   /**
    * Removes a set of keys. Returns the keys that were removed.
    *
    * @param keys
    * @return
    */
   Multi<? extends CacheEntry<K, V>> getAndRemoveMany(Multi<K> keys);

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   Uni<Long> estimateSize();

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work
    *
    * @return Void
    */
   Uni<Void> clear();

   /**
    * Executes the query and returns a {@link Multi} with the results
    *
    * @param query query String
    * @return a {@link Multi} which produces query result items.
    */
   <R> Multi<R> find(String query);

   /**
    * Find by QueryRequest.
    *
    * @param query
    * @return
    */
   <R> MutinyQuery<R> query(String query);

   /**
    * Executes the query and returns a {@link Multi} with the results
    *
    * @param query query String
    * @return a {@link Multi} which produces {@link CacheContinuousQueryEvent} items.
    */
   <R> Multi<CacheContinuousQueryEvent<K, R>> queryContinuously(String query);

   /**
    * Listens to the events
    *
    * @param types
    * @return a {@link Multi} which produces {@link CacheEntryEvent} items.
    */
   default Multi<? extends CacheEntryEvent<K, V>> listen(CacheEntryEventType... types) {
      return listen(new CacheListenerOptions(), types);
   }

   /**
    * Listens to the events
    *
    * @param options
    * @param types
    * @return a {@link Multi} which produces {@link CacheEntryEvent} items.
    */
   Multi<? extends CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types);

   /**
    * @param key
    * @param remappingFunction
    * @return
    */
   Uni<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

   /**
    * @param key
    * @param mappingFunction
    * @return
    */
   Uni<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

   /**
    * Process all entries using the supplied task
    * @param task
    */
   Multi<MutinyWriteResult> process(Set<K> keys, EntryConsumerTask<K, V> task); // cache.process(new ServerTask("blah"));

   /**
    * NO PROJECTIONS !
    * @param query
    * @param task
    * @return
    */
   Multi<MutinyWriteResult> process(String query, EntryConsumerTask<K, V> task);

   MutinyStreamingCache<K> streaming();
}

