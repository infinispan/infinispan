package org.infinispan.interceptors.distribution;

import org.infinispan.commons.CacheException;

/**
 * Thrown when there's no owner of given segment in current topology. That may happen after node crash
 * (and we need to wait until rebalance starts - until new topology) or during a partition, when
 * {@link org.infinispan.partitionhandling.AvailabilityException} should be thrown as soon as we become
 * degraded.
 */
public class MissingOwnerException extends CacheException {
   private final int topologyId;

   public MissingOwnerException(int topologyId) {
      super(null, null, false, false);
      this.topologyId = topologyId;
   }

   public int getTopologyId() {
      return topologyId;
   }
}
