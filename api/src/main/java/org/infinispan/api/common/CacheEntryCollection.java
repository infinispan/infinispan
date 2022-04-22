package org.infinispan.api.common;

import java.util.Collection;

/**
 * @since 14.0
 **/
public interface CacheEntryCollection<K, V> {
   K key();

   Collection<V> values();

   CacheEntryMetadata metadata();
}
