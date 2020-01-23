package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.util.PeekableMap;
import org.infinispan.container.entries.InternalCacheEntry;

public interface PeekableTouchableMap<K, V> extends PeekableMap<K, InternalCacheEntry<K, V>>, ConcurrentMap<K, InternalCacheEntry<K, V>> {
   /**
    * Touches the entry for the given key in this map. This method will update any recency timestamps for both
    * expiration or eviction as needed.
    * @param key key to touch
    * @param currentTimeMillis the recency timestamp to set
    * @return whether the entry was touched or not
    */
   boolean touchKey(Object key, long currentTimeMillis);
}
