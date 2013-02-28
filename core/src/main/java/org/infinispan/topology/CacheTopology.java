package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static Log log = LogFactory.getLog(CacheTopology.class);
   private static final boolean trace = log.isTraceEnabled();

   private final int topologyId;
   private final ConsistentHash currentCH;
   private final ConsistentHash pendingCH;

   public CacheTopology(int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) {
      if (pendingCH != null && !pendingCH.getMembers().containsAll(currentCH.getMembers())) {
         throw new IllegalArgumentException("A cache topology's pending consistent hash must " +
               "contain all the current consistent hash's members");
      }
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
         return InfinispanCollections.emptyList();
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheTopology that = (CacheTopology) o;

      if (topologyId != that.topologyId) return false;
      if (currentCH != null ? !currentCH.equals(that.currentCH) : that.currentCH != null) return false;
      if (pendingCH != null ? !pendingCH.equals(that.pendingCH) : that.pendingCH != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + (currentCH != null ? currentCH.hashCode() : 0);
      result = 31 * result + (pendingCH != null ? pendingCH.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CacheTopology{" +
            "id=" + topologyId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            '}';
   }

   public final void logRoutingTableInformation() {
      if (trace) {
         log.tracef("Current consistent hash's routing table: %s", currentCH.getRoutingTableAsString());
         if (pendingCH != null) log.tracef("Pending consistent hash's routing table: %s", pendingCH.getRoutingTableAsString());
      }
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
         return Ids.CACHE_TOPOLOGY;
      }

      @Override
      public Set<Class<? extends CacheTopology>> getTypeClasses() {
         return Collections.<Class<? extends CacheTopology>>singleton(CacheTopology.class);
      }
   }
}
