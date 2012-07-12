package org.infinispan.topology;

import org.infinispan.distribution.newch.AdvancedConsistentHash;
import org.infinispan.distribution.newch.ConsistentHash;

/**
 * The status of a cache from a distribution/state transfer point of view.
 * The pending CH can be {@code null} if we don't have a state transfer in progress.
 *
 * @author Dan Berindei
 * @since 5.2
 */
class CacheTopology {
   int version;
   ConsistentHash currentCH;
   ConsistentHash pendingCH;
}
