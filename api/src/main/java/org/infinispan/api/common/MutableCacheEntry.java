package org.infinispan.api.common;

/**
 * @since 14.0
 **/
public interface MutableCacheEntry<K, V> extends CacheEntry<K, V> {
   void setValue(V newValue);
}
