package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.metadata.Metadata;

/**
 * A Filter that only allows post events to be accepted.
 *
 * @author wburns
 * @since 7.0
 */
public class PostCacheEventFilter<K, V> implements CacheEventFilter<K, V> {
   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return !eventType.isPreEvent();
   }
}
