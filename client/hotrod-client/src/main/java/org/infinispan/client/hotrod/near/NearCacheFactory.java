package org.infinispan.client.hotrod.near;

import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;

/**
 * @since 14.0
 **/
public interface NearCacheFactory {
   <K,V> NearCache<K, V> createNearCache(NearCacheConfiguration config, BiConsumer<K, MetadataValue<V>> removedConsumer);
}
