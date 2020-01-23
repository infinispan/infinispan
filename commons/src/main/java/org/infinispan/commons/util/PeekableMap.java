package org.infinispan.commons.util;

import java.util.Map;

/**
 *
 * @param <K>
 * @param <V>
 * @deprecated since 11.0 with no replacement - no longer used
 */
@Deprecated
public interface PeekableMap<K, V> extends Map<K, V> {
   /**
    * Peaks at a value for the given key.  Note that this does not update any expiration or
    * eviction information when this is performed on the map, unlike the get method.
    * @param key The key to find the value for
    * @return The value mapping to this key
    */
   public V peek(Object key);
}
