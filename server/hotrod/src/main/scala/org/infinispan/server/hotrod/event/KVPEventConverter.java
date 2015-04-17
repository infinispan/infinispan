package org.infinispan.server.hotrod.event;

import org.infinispan.commons.event.KVPEvent;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;

public class KVPEventConverter<K, V> implements CacheEventConverter<K, V, KVPEvent<K, V>> {
   @Override
   public KVPEvent<K, V> convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return new KVPEvent<K, V>(key, newValue, oldValue);
   }
}
