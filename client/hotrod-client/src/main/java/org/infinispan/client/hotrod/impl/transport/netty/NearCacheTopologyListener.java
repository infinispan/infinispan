package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.BitSet;

import org.infinispan.client.hotrod.impl.topology.CacheInfo;

/**
 * Coordinates near-cache invalidation and listener registration with topology publication.
 * Callbacks are serialized with topology updates and must not block.
 */
public interface NearCacheTopologyListener {
   /**
    * Invalidates entries affected by the new owners, before publishing the topology. Invoked under the topology
    * write lock; implementations must not call dispatcher methods that acquire the topology lock.
    */
   void beforeTopologyUpdate(CacheInfo previous, CacheInfo next);

   /**
    * Invalidates segments whose primary owner changed between sequenced pushes, even if the final owners are
    * unchanged. A null set means the history is unknown and all segments must be invalidated. Invoked before
    * publication under the topology write lock, with the same restrictions as {@link #beforeTopologyUpdate}.
    */
   void primaryOwnersChanged(BitSet changedSegments);

   /**
    * Reconciles listener registrations after publication, outside the topology write lock.
    */
   void afterTopologyUpdate(CacheInfo previous, CacheInfo next);
}
