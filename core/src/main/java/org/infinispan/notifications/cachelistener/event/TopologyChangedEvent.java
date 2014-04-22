package org.infinispan.notifications.cachelistener.event;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;

/**
 * The event passed in to methods annotated with {@link TopologyChanged}.
 *
 * @author Manik Surtani
 * @since 5.0
 */
public interface TopologyChangedEvent<K, V> extends Event<K, V> {

   /**
    * @return retrieves the consistent hash at the start of a topology change
    */
   ConsistentHash getConsistentHashAtStart();

   /**
    * @return retrieves the consistent hash at the end of a topology change
    */
   ConsistentHash getConsistentHashAtEnd();

   int getNewTopologyId();
}
