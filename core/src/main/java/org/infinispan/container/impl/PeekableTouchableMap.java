package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.container.entries.InternalCacheEntry;

public interface PeekableTouchableMap<K, V> extends ConcurrentMap<K, InternalCacheEntry<K, V>> {

   /**
    * Peaks at a value for the given key.  Note that this does not update any expiration or
    * eviction information when this is performed on the map, unlike the get method.
    * @param key The key to find the value for
    * @return The value mapping to this key
    */
   InternalCacheEntry<K, V> peek(Object key);

   /**
    * Touches the entry for the given key in this map. This method will update any recency timestamps for both
    * expiration or eviction as needed.
    * @param key key to touch
    * @param currentTimeMillis the recency timestamp to set
    * @return whether the entry was touched or not
    */
   boolean touchKey(Object key, long currentTimeMillis);

   /**
    * Touches all entries in the map setting the recency timestamps for both expiration eviction appropriately.
    * @param currentTimeMillis the recency timestamp to set
    */
   void touchAll(long currentTimeMillis);

   /**
    * Same as {@link #put(Object, Object)} except that the map is not required to return a value. This can be useful
    * when retrieving a previous value may incur additional costs.
    * <p>
    * @implSpec The default implementation is equivalent to, for this
    * {@code map}:
    * <pre> {@code
    * map.put(key, value);
    * }}</pre>
    * @param key key to insert for the value
    * @param value the value to insert into this map
    */
   default void putNoReturn(K key, InternalCacheEntry<K, V> value) {
      put(key, value);
   }
}
