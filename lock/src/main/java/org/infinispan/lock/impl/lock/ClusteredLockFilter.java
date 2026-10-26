package org.infinispan.lock.impl.lock;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Filter used by the single cluster listener that every node registers on the clustered lock cache. It only accepts
 * the events that the node is interested in, which keeps the per lock notification traffic down to a unicast between
 * the node owning the lock key and the nodes waiting for the lock.
 * <p>
 * A node is interested in an event when it is registered as a waiter of the lock, see
 * {@link ClusteredLockValue#waiterId(org.infinispan.remoting.transport.Address)}, and either the lock has just been
 * released or the lock has been removed.
 * <p>
 * More about listeners {@see http://infinispan.org/docs/stable/user_guide/user_guide.html#cache_level_notifications}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FILTER)
public class ClusteredLockFilter implements CacheEventFilter<ClusteredLockKey, ClusteredLockValue> {

   @ProtoField(value = 1, defaultValue = "0")
   final int waiterId;

   @ProtoFactory
   public ClusteredLockFilter(int waiterId) {
      this.waiterId = waiterId;
   }

   @Override
   public boolean accept(ClusteredLockKey key, ClusteredLockValue oldValue, Metadata oldMetadata, ClusteredLockValue newValue, Metadata newMetadata, EventType eventType) {
      if (eventType.isRemove()) {
         // The pending requests have to be completed exceptionally when the lock is removed. The removed value is
         // the only place where the waiters are still available.
         return oldValue != null && oldValue.hasWaiter(waiterId);
      }
      return newValue != null && newValue.getState() == ClusteredLockState.RELEASED && newValue.hasWaiter(waiterId);
   }

   @Override
   public String toString() {
      return "ClusteredLockFilter{waiterId=" + waiterId + '}';
   }
}
