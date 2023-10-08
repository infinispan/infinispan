package org.infinispan.lock.impl.lock;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * This listener is used to monitor lock state changes.
 * More about listeners {@see http://infinispan.org/docs/stable/user_guide/user_guide.html#cache_level_notifications}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FILTER)
public class ClusteredLockFilter implements CacheEventFilter<ClusteredLockKey, ClusteredLockValue> {

   @ProtoField(1)
   final ClusteredLockKey name;

   @ProtoFactory
   public ClusteredLockFilter(ClusteredLockKey name) {
      this.name = name;
   }

   @Override
   public boolean accept(ClusteredLockKey key, ClusteredLockValue oldValue, Metadata oldMetadata, ClusteredLockValue newValue, Metadata newMetadata, EventType eventType) {
      return name.equals(key);
   }
}
