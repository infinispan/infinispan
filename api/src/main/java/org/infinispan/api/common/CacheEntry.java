package org.infinispan.api.common;

/**
 * @since 14.0
 **/
public interface CacheEntry<K, V> {
   K key();

   V value();

   CacheEntryMetadata metadata();
}
