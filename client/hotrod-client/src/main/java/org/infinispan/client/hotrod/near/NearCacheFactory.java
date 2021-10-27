package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

/**
 * @since 14.0
 **/
public interface NearCacheFactory {
   <K,V> NearCache<K, V> createNearCache(NearCacheConfiguration config);
}
