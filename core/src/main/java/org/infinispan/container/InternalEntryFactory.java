package org.infinispan.container;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A factory for {@link InternalCacheEntry} and {@link InternalCacheValue} instances.
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface InternalEntryFactory {

   /**
    * Creates a new {@link InternalCacheEntry} instance based on the key, value, version and timestamp/lifespan
    * information reflected in the {@link CacheEntry} instance passed in.
    * @param cacheEntry cache entry to copy
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   <K, V> InternalCacheEntry<K, V> create(CacheEntry<K, V> cacheEntry);

   /**
    * Creates a new {@link InternalCacheEntry} instance based on the version and timestamp/lifespan
    * information reflected in the {@link CacheEntry} instance passed in.  Key and value are both passed in 
    * explicitly.
    * @param key key to use
    * @param value value to use
    * @param cacheEntry cache entry to retrieve version and timestamp/lifespan information from
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   <K, V> InternalCacheEntry<K, V> create(K key, V value, InternalCacheEntry<?, ?> cacheEntry);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use           
    * @param metadata metadata for entry
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   <K, V> InternalCacheEntry<K, V> create(K key, V value, Metadata metadata);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param metadata metadata for entry
    * @param lifespan lifespan to use
    * @param maxIdle maxIdle to use
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   <K, V> InternalCacheEntry<K, V> create(K key, V value, Metadata metadata, long lifespan, long maxIdle);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param metadata metadata for entry
    * @param created creation timestamp to use
    * @param lifespan lifespan to use
    * @param lastUsed lastUsed timestamp to use
    * @param maxIdle maxIdle to use
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   <K, V> InternalCacheEntry<K, V> create(K key, V value, Metadata metadata, long created, long lifespan, long lastUsed, long maxIdle);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param version version to use
    * @param created creation timestamp to use
    * @param lifespan lifespan to use
    * @param lastUsed lastUsed timestamp to use
    * @param maxIdle maxIdle to use
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry
    */
   // To be deprecated, once metadata object can be retrieved remotely...
   <K, V> InternalCacheEntry<K, V> create(K key, V value, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle);

   /**
    * TODO: Adjust javadoc
    *
    * Updates an existing {@link InternalCacheEntry} with new metadata.  This may result in a new
    * {@link InternalCacheEntry} instance being created, as a different {@link InternalCacheEntry} implementation
    * may be more appropriate to suit the new metadata values.  As such, one should consider the {@link InternalCacheEntry}
    * passed in as a parameter as passed by value and not by reference.
    * 
    * @param cacheEntry original internal cache entry
    * @param metadata new metadata
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry instance
    */
   <K, V> InternalCacheEntry<K, V> update(InternalCacheEntry<K, V> cacheEntry, Metadata metadata);

   /**
    * Similar to {@link #update(org.infinispan.container.entries.InternalCacheEntry, org.infinispan.metadata.Metadata)}
    * but it also updates the {@link org.infinispan.container.entries.InternalCacheEntry} value.
    * <p/>
    * If the same internal cache entry is returned and if it is a mortal cache entry, the returned instance needs to be
    * reincarnated.
    *
    * @param cacheEntry original internal cache entry
    * @param value      new value
    * @param metadata   new metadata
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    * @return a new InternalCacheEntry instance or the existing original
    */
   <K, V> InternalCacheEntry<K, V> update(InternalCacheEntry<K, V> cacheEntry, V value, Metadata metadata);

   /**
    * Creates an {@link InternalCacheValue} based on the {@link InternalCacheEntry} passed in.
    * 
    * @param cacheEntry to use to generate a {@link InternalCacheValue}
    * @param <V> The value type
    * @return an {@link InternalCacheValue}
    */
   <V> InternalCacheValue<V> createValue(CacheEntry<?, V> cacheEntry);

   /**
    * Creates a copy of this cache entry and synchronizes serializes the copy process with the {@link #update(org.infinispan.container.entries.InternalCacheEntry, org.infinispan.metadata.Metadata)}.
    * This is requires so that readers of the entry will get an consistent snapshot of the value red.
    * @param <K> The key type for the entry
    * @param <V> The value type for the entry
    */
   <K, V> CacheEntry<K, V> copy(CacheEntry<K, V> cacheEntry);
}
