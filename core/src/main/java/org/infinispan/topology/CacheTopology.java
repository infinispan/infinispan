package org.infinispan.topology;

import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupingConsistentHash;
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
 * @author Pedro Ruivo
 * @since 5.2
 */
public class CacheTopology {

   private static Log log = LogFactory.getLog(CacheTopology.class);
   private static final boolean trace = log.isTraceEnabled();

   private final int topologyId;
   private final int rebalanceId;
   private final ConsistentHash readCH;
   private final ConsistentHash writeCH;
   private List<Address> actualMembers;

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash readCH, ConsistentHash writeCH,
         List<Address> actualMembers) {
      if (readCH == null) {
         throw new NullPointerException("Read Consistent Hash must be non null.");
      }
      if (writeCH == null) {
         throw new NullPointerException("Write Consistent Hash must be non null.");
      }
      if (!writeCH.getMembers().containsAll(readCH.getMembers())) {
         throw new IllegalArgumentException("A cache topology's pending consistent hash must " +
               "contain all the current consistent hash's members");
      }
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.readCH = readCH;
      this.writeCH = writeCH;
      this.actualMembers = actualMembers;
   }

   public int getTopologyId() {
      return topologyId;
   }

   /**
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} to read. It returns the same instance as {@link
    * #getWriteConsistentHash()} when no rebalance is in progress.
    */
   public ConsistentHash getReadConsistentHash() {
      return readCH;
   }

   /**
    * @return the {@link org.infinispan.distribution.ch.ConsistentHash} to write. It returns the same instance as {@link
    * #getReadConsistentHash()} when no rebalance is in progress.
    */
   public ConsistentHash getWriteConsistentHash() {
      return writeCH;
   }

   /**
    * The id of the latest started rebalance.
    */
   public int getRebalanceId() {
      return rebalanceId;
   }

   /**
    * @return The nodes that are members in write consistent hash
    * @see {@link #getActualMembers()}
    */
   public List<Address> getMembers() {
      return writeCH.getMembers();
   }

   /**
    * @return The nodes that are active members of the cache. It should be equal to {@link #getMembers()} when the
    *    cache is available, and a strict subset if the cache is in degraded mode.
    * @see org.infinispan.partitionhandling.AvailabilityMode
    */
   public List<Address> getActualMembers() {
      return actualMembers;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheTopology that = (CacheTopology) o;

      if (topologyId != that.topologyId) return false;
      if (rebalanceId != that.rebalanceId) return false;
      if (!readCH.equals(that.readCH)) return false;
      if (!writeCH.equals(that.writeCH)) return false;
      if (actualMembers != null ? !actualMembers.equals(that.actualMembers) : that.actualMembers != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + rebalanceId;
      result = 31 * result + readCH.hashCode();
      result = 31 * result + writeCH.hashCode();
      result = 31 * result + (actualMembers != null ? actualMembers.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CacheTopology{" +
            "id=" + topologyId +
            ", rebalanceId=" + rebalanceId +
            ", readCH=" + readCH +
            ", writeCH=" + writeCH +
            ", actualMembers=" + actualMembers +
            '}';
   }

   public final void logRoutingTableInformation() {
      if (trace) {
         log.tracef("Read consistent hash's routing table: %s", readCH.getRoutingTableAsString());
         log.tracef("Write consistent hash's routing table: %s", writeCH.getRoutingTableAsString());
      }
   }

   public final CacheTopology addGrouping(GroupManager groupManager) {
      if (readCH == writeCH) {
         ConsistentHash newCH = new GroupingConsistentHash(readCH, groupManager);
         return new CacheTopology(topologyId, rebalanceId, newCH, newCH, actualMembers);
      }
      return new CacheTopology(topologyId, rebalanceId, new GroupingConsistentHash(readCH, groupManager),
                               new GroupingConsistentHash(writeCH, groupManager), actualMembers);
   }

   /**
    * It checks if both consistent hashes are the same.
    * <p/>
    * If no rebalance is in progress, the read and the write consistent hash are the same.
    *
    * @return {@code true} if the read and write consistent hash are the same, {@code false} otherwise.
    */
   public final boolean isStable() {
      return readCH.equals(writeCH);
   }

   public static class Externalizer extends InstanceReusingAdvancedExternalizer<CacheTopology> {
      @Override
      public void doWriteObject(ObjectOutput output, CacheTopology cacheTopology) throws IOException {
         output.writeInt(cacheTopology.topologyId);
         output.writeInt(cacheTopology.rebalanceId);
         boolean stable = cacheTopology.readCH.equals(cacheTopology.writeCH);
         if (stable) {
            output.writeBoolean(true);
            output.writeObject(cacheTopology.readCH);
         } else {
            output.writeBoolean(false);
            output.writeObject(cacheTopology.readCH);
            output.writeObject(cacheTopology.writeCH);
         }
         output.writeObject(cacheTopology.actualMembers);
      }

      @Override
      public CacheTopology doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int topologyId = unmarshaller.readInt();
         int rebalanceId = unmarshaller.readInt();
         ConsistentHash readCH;
         ConsistentHash writeCH;
         if (unmarshaller.readBoolean()) {
            readCH = (ConsistentHash) unmarshaller.readObject();
            writeCH = readCH;
         } else {
            readCH = (ConsistentHash) unmarshaller.readObject();
            writeCH = (ConsistentHash) unmarshaller.readObject();
         }
         List<Address> actualMembers = (List<Address>) unmarshaller.readObject();
         return new CacheTopology(topologyId, rebalanceId, readCH, writeCH, actualMembers);
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
