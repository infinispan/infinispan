package org.infinispan.server.hotrod.event;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.KeyValueWithPrevious;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_KEY_VALUE_WITH_PREVIOUS_CONVERTER)
public class KeyValueWithPreviousEventConverter<K, V> implements CacheEventConverter<K, V, KeyValueWithPrevious<K, V>> {
   @Override
   public KeyValueWithPrevious<K, V> convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return new KeyValueWithPrevious<>(key, newValue, oldValue);
   }
}
