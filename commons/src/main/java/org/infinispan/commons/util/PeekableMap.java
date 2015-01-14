package org.infinispan.commons.util;

import java.util.Map;

public interface PeekableMap<K, V> extends Map<K, V> {
   /**
    * Peaks at a value for the given key.  Note that this does not update any expiration or
    * eviction information when this is performed on the map, unlike the get method.
    * @param key The key to find the value for
    * @return The value mapping to this key
    */
   public V peek(Object key);
}
