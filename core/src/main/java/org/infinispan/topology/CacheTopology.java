package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

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
public class CacheTopology {
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

   /**
    * The current consistent hash.
    */
   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   /**
    * The future consistent hash. Should be {@code null} if there is no rebalance in progress.
    */
   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   public List<Address> getMembers() {
      if (pendingCH != null)
         return pendingCH.getMembers();
      else if (currentCH != null)
         return currentCH.getMembers();
      else
         return Collections.emptyList();
   }

   /**
    * Read operations should always go to the "current" members.
    */
   public ConsistentHash getReadConsistentHash() {
      return currentCH;
   }

   /**
    * When there is a rebalance in progress, write operations should go to the "pending" members.
    *
    * Note: The pending members always include the current members (unless there is no rebalance in progress).
    */
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


   public static class Externalizer extends AbstractExternalizer<CacheTopology> {
      @Override
      public void writeObject(ObjectOutput output, CacheTopology cacheTopology) throws IOException {
         output.writeInt(cacheTopology.topologyId);
         output.writeObject(cacheTopology.currentCH);
         output.writeObject(cacheTopology.pendingCH);
      }

      @Override
      public CacheTopology readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int topologyId = unmarshaller.readInt();
         ConsistentHash currentCH = (ConsistentHash) unmarshaller.readObject();
         ConsistentHash pendingCH = (ConsistentHash) unmarshaller.readObject();
         return new CacheTopology(topologyId, currentCH, pendingCH);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_VIEW;
      }

      @Override
      public Set<Class<? extends CacheTopology>> getTypeClasses() {
         return Util.<Class<? extends CacheTopology>>asSet(CacheTopology.class);
      }
   }
}
