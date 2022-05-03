package org.infinispan.hotrod.configuration;

import java.util.function.Consumer;

import org.infinispan.api.common.CacheEntry;

/**
 * @since 14.0
 **/
public interface NearCacheFactory {
   <K,V> NearCache<K, V> createNearCache(NearCacheConfiguration config, Consumer<CacheEntry<K, V>> removedConsumer);
}
