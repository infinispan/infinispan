package org.infinispan.persistence.remote.upgrade;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_REMOVED_FILTER)
public class RemovedFilter<K, V> implements CacheEventFilter<K, V> {

   @Override
   public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata,
                         EventType eventType) {
      return eventType.isRemove();
   }

   @Override
   public MediaType format() {
      return null;
   }
}
