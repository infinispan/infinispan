package org.infinispan.topology;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

/**
 * The status of a cache from a distribution/state transfer point of view.
 * <p/>
 * The pending CH can be {@code null} if we don't have a state transfer in progress.
 * <p/>
 * The {@code topologyId} is incremented every time the topology changes (i.e. state transfer
 * starts or ends). It is not modified when the consistent hashes are updated without requiring state
 * transfer (e.g. when a member leaves).
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheTopology implements Serializable {
   private final int topologyId;
   private final ConsistentHash currentCH;
   private final ConsistentHash pendingCH;

   public CacheTopology(int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) {
      this.topologyId = topologyId;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   public Collection<Address> getMembers() {
      if (pendingCH != null)
         return pendingCH.getMembers();
      else if (currentCH != null)
         return currentCH.getMembers();
      else
         return Collections.emptyList();
   }

   public ConsistentHash getReadConsistentHash() {
      return currentCH;
   }

   public ConsistentHash getWriteConsistentHash() {
      if (pendingCH != null)
         return pendingCH;
      else
         return currentCH;
   }

   @Override
   public String toString() {
      return "CacheTopology{" +
            "topologyId=" + topologyId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            '}';
   }
}
