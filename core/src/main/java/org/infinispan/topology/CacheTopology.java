package org.infinispan.topology;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The status of a cache from a distribution/state transfer point of view.
 * <p/>
 * The pending CH can be {@code null} if we don't have a state transfer in progress.
 * <p/>
 * The {@code topologyId} is incremented every time the topology changes (e.g. a member leaves, state transfer
 * starts or ends).
 * The {@code rebalanceId} is not modified when the consistent hashes are updated without requiring state
 * transfer (e.g. when a member leaves).
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheTopology {

   private static Log log = LogFactory.getLog(CacheTopology.class);
   private static final boolean trace = log.isTraceEnabled();

   private final int topologyId;
   private final int rebalanceId;
   private final ConsistentHash currentCH;
   private final ConsistentHash pendingCH;
   private final transient ConsistentHash unionCH;
   private List<Address> actualMembers;

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash currentCH, ConsistentHash pendingCH,
         List<Address> actualMembers) {
      this(topologyId, rebalanceId, currentCH, pendingCH, null, actualMembers);
   }

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash currentCH, ConsistentHash pendingCH,
         ConsistentHash unionCH, List<Address> actualMembers) {
      if (pendingCH != null && !pendingCH.getMembers().containsAll(currentCH.getMembers())) {
         throw new IllegalArgumentException("A cache topology's pending consistent hash must " +
               "contain all the current consistent hash's members");
      }
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
      this.unionCH = unionCH;
      this.actualMembers = actualMembers;
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

   /**
    * The union of the current and future consistent hashes. Should be {@code null} if there is no rebalance in progress.
    */
   public ConsistentHash getUnionCH() {
      return unionCH;
   }

   /**
    * The id of the latest started rebalance.
    */
   public int getRebalanceId() {
      return rebalanceId;
   }

   /**
    * @return The nodes that are members in both consistent hashes (if {@code pendingCH != null},
    *    otherwise the members of the current CH).
    * @see #getActualMembers()
    */
   public List<Address> getMembers() {
      if (pendingCH != null)
         return pendingCH.getMembers();
      else if (currentCH != null)
         return currentCH.getMembers();
      else
         return InfinispanCollections.emptyList();
   }

   /**
    * @return The nodes that are active members of the cache. It should be equal to {@link #getMembers()} when the
    *    cache is available, and a strict subset if the cache is in degraded/unavailable mode.
    * @see org.infinispan.partitionhandling.AvailabilityMode
    */
   public List<Address> getActualMembers() {
      return actualMembers;
   }

   /**
    * Read operations should always go to the "current" owners.
    */
   public ConsistentHash getReadConsistentHash() {
      return currentCH;
   }

   /**
    * When there is a rebalance in progress, write operations should go to the union of the "current" and "future" owners.
    */
   public ConsistentHash getWriteConsistentHash() {
      if (pendingCH != null) {
         if (unionCH == null)
            throw new IllegalStateException("Need a union CH when a pending CH is set");
         return unionCH;
      }

      return currentCH;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheTopology that = (CacheTopology) o;

      if (topologyId != that.topologyId) return false;
      if (rebalanceId != that.rebalanceId) return false;
      if (currentCH != null ? !currentCH.equals(that.currentCH) : that.currentCH != null) return false;
      if (pendingCH != null ? !pendingCH.equals(that.pendingCH) : that.pendingCH != null) return false;
      if (unionCH != null ? !unionCH.equals(that.unionCH) : that.unionCH != null) return false;
      if (actualMembers != null ? !actualMembers.equals(that.actualMembers) : that.actualMembers != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + rebalanceId;
      result = 31 * result + (currentCH != null ? currentCH.hashCode() : 0);
      result = 31 * result + (pendingCH != null ? pendingCH.hashCode() : 0);
      result = 31 * result + (unionCH != null ? unionCH.hashCode() : 0);
      result = 31 * result + (actualMembers != null ? actualMembers.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CacheTopology{" +
            "id=" + topologyId +
            ", rebalanceId=" + rebalanceId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            ", unionCH=" + unionCH +
            ", actualMembers=" + actualMembers +
            '}';
   }

   public final void logRoutingTableInformation() {
      if (trace) {
         log.tracef("Current consistent hash's routing table: %s", currentCH.getRoutingTableAsString());
         if (pendingCH != null) log.tracef("Pending consistent hash's routing table: %s", pendingCH.getRoutingTableAsString());
         if (unionCH != null) log.tracef("Union consistent hash's routing table: %s", unionCH.getRoutingTableAsString());
      }
   }


   public static class Externalizer extends InstanceReusingAdvancedExternalizer<CacheTopology> {
      @Override
      public void doWriteObject(ObjectOutput output, CacheTopology cacheTopology) throws IOException {
         output.writeInt(cacheTopology.topologyId);
         output.writeInt(cacheTopology.rebalanceId);
         output.writeObject(cacheTopology.currentCH);
         output.writeObject(cacheTopology.pendingCH);
         output.writeObject(cacheTopology.unionCH);
         output.writeObject(cacheTopology.actualMembers);
      }

      @Override
      public CacheTopology doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int topologyId = unmarshaller.readInt();
         int rebalanceId = unmarshaller.readInt();
         ConsistentHash currentCH = (ConsistentHash) unmarshaller.readObject();
         ConsistentHash pendingCH = (ConsistentHash) unmarshaller.readObject();
         ConsistentHash unionCH = (ConsistentHash) unmarshaller.readObject();
         List<Address> actualMembers = (List<Address>) unmarshaller.readObject();
         return new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH, unionCH, actualMembers);
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
