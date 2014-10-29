package org.infinispan.notifications.cachelistener.event;

import org.infinispan.partionhandling.AvailabilityMode;

/**
 * The event passed in to methods annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged}.
 *
 * @author William Burns
 * @since 7.0
 */
public interface PartitionStatusChangedEvent<K, V> extends Event<K, V> {

   /**
    * The mode the current cluster is in.  This determines which operations can be ran in the cluster.
    * @return the mode
    */
   AvailabilityMode getAvailabilityMode();
}
