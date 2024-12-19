package org.infinispan.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
@ProtoTypeId(ProtoStreamTypeIds.CACHE_TOPOLOGY)
public class CacheTopology {

   private static final Log log = LogFactory.getLog(CacheTopology.class);

   private final int topologyId;
   private final int rebalanceId;
   private final boolean restoredFromState;
   private final ConsistentHash currentCH;
   private final ConsistentHash pendingCH;
   private final ConsistentHash unionCH;
   private final Phase phase;
   private final List<Address> actualMembers;
   // The persistent UUID of each actual member
   private final List<PersistentUUID> persistentUUIDs;

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash currentCH, ConsistentHash pendingCH,
                        Phase phase, List<Address> actualMembers, List<PersistentUUID> persistentUUIDs) {
      this(topologyId, rebalanceId, currentCH, pendingCH, null, phase, actualMembers, persistentUUIDs);
   }

   public CacheTopology(int topologyId, int rebalanceId, boolean restoredTopology, ConsistentHash currentCH, ConsistentHash pendingCH,
                        Phase phase, List<Address> actualMembers, List<PersistentUUID> persistentUUIDs) {
      this(topologyId, rebalanceId, restoredTopology, currentCH, pendingCH, null, phase, actualMembers, persistentUUIDs);
   }

   public CacheTopology(int topologyId, int rebalanceId, ConsistentHash currentCH, ConsistentHash pendingCH,
                        ConsistentHash unionCH, Phase phase, List<Address> actualMembers, List<PersistentUUID> persistentUUIDs) {
      this(topologyId, rebalanceId, false, currentCH, pendingCH, unionCH, phase, actualMembers, persistentUUIDs);
   }

   public CacheTopology(int topologyId, int rebalanceId, boolean restoredTopology, ConsistentHash currentCH, ConsistentHash pendingCH,
                        ConsistentHash unionCH, Phase phase, List<Address> actualMembers, List<PersistentUUID> persistentUUIDs) {
      if (pendingCH != null && !pendingCH.getMembers().containsAll(currentCH.getMembers())) {
         throw new IllegalArgumentException("A cache topology's pending consistent hash must " +
               "contain all the current consistent hash's members: currentCH=" + currentCH + ", pendingCH=" + pendingCH);
      }
      if (persistentUUIDs != null && persistentUUIDs.size() != actualMembers.size()) {
         throw new IllegalArgumentException("There must be one persistent UUID for each actual member");
      }
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
      this.unionCH = unionCH;
      this.phase = phase;
      this.actualMembers = actualMembers;
      this.persistentUUIDs = persistentUUIDs;
      this.restoredFromState = restoredTopology;
   }

   @ProtoFactory
   CacheTopology(int topologyId, int rebalanceId, boolean restoredFromState, Phase phase, List<PersistentUUID> membersPersistentUUIDs,
                 MarshallableObject<ConsistentHash> wrappedCurrentCH, MarshallableObject<ConsistentHash> wrappedPendingCH,
                 MarshallableObject<ConsistentHash> wrappedUnionCH, List<JGroupsAddress> jGroupsMembers) {
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.restoredFromState = restoredFromState;
      this.currentCH = MarshallableObject.unwrap(wrappedCurrentCH);
      this.pendingCH = MarshallableObject.unwrap(wrappedPendingCH);
      this.unionCH = MarshallableObject.unwrap(wrappedUnionCH);
      this.phase = phase;
      this.persistentUUIDs = membersPersistentUUIDs;
      this.actualMembers = (List<Address>)(List<?>) jGroupsMembers;
   }

   @ProtoField(number = 1, defaultValue = "-1")
   public int getTopologyId() {
      return topologyId;
   }

   /**
    * The id of the latest started rebalance.
    */
   @ProtoField(number = 2, defaultValue = "-1")
   public int getRebalanceId() {
      return rebalanceId;
   }

   @ProtoField(number = 3, defaultValue = "false")
   boolean getRestoredFromState() {
      return restoredFromState;
   }

   @ProtoField(4)
   public Phase getPhase() {
      return phase;
   }

   @ProtoField(number = 5, collectionImplementation = ArrayList.class)
   public List<PersistentUUID> getMembersPersistentUUIDs() {
      return persistentUUIDs;
   }

   @ProtoField(number = 6, name = "currentCH")
   MarshallableObject<ConsistentHash> getWrappedCurrentCH() {
      return MarshallableObject.create(currentCH);
   }

   @ProtoField(number = 7, name = "pendingCH")
   MarshallableObject<ConsistentHash> getWrappedPendingCH() {
      return MarshallableObject.create(pendingCH);
   }

   @ProtoField(number = 8, name = "unionCH")
   MarshallableObject<ConsistentHash> getWrappedUnionCH() {
      return MarshallableObject.create(unionCH);
   }

   @ProtoField(number = 9, collectionImplementation = ArrayList.class)
   List<JGroupsAddress> getJGroupsMembers() {
      return (List<JGroupsAddress>)(List<?>) actualMembers;
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
         return Collections.emptyList();
   }

   /**
    * @return The nodes that are active members of the cache. It should be equal to {@link #getMembers()} when the
    *    cache is available, and a strict subset if the cache is in degraded mode.
    * @see org.infinispan.partitionhandling.AvailabilityMode
    */
   public List<Address> getActualMembers() {
      return actualMembers;
   }

   public boolean wasTopologyRestoredFromState() {
      return restoredFromState;
   }

   /**
    * Read operations should always go to the "current" owners.
    */
   public ConsistentHash getReadConsistentHash() {
      switch (phase) {
         case CONFLICT_RESOLUTION:
         case NO_REBALANCE:
            assert pendingCH == null;
            assert unionCH == null;
            return currentCH;
         case READ_OLD_WRITE_ALL:
            assert pendingCH != null;
            assert unionCH != null;
            return currentCH;
         case READ_ALL_WRITE_ALL:
            assert pendingCH != null;
            return unionCH;
         case READ_NEW_WRITE_ALL:
            assert unionCH != null;
            return pendingCH;
         default:
            throw new IllegalStateException();
      }
   }

   /**
    * When there is a rebalance in progress, write operations should go to the union of the "current" and "future" owners.
    */
   public ConsistentHash getWriteConsistentHash() {
      switch (phase) {
         case CONFLICT_RESOLUTION:
         case NO_REBALANCE:
            assert pendingCH == null;
            assert unionCH == null;
            return currentCH;
         case READ_OLD_WRITE_ALL:
         case READ_ALL_WRITE_ALL:
         case READ_NEW_WRITE_ALL:
            assert pendingCH != null;
            assert unionCH != null;
            return unionCH;
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheTopology that = (CacheTopology) o;

      if (topologyId != that.topologyId) return false;
      if (rebalanceId != that.rebalanceId) return false;
      if (phase != that.phase) return false;
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
      result = 31 * result + (phase != null ? phase.hashCode() : 0);
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
            ", phase=" + phase +
            ", rebalanceId=" + rebalanceId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            ", unionCH=" + unionCH +
            ", actualMembers=" + actualMembers +
            ", persistentUUIDs=" + persistentUUIDs +
            '}';
   }

   public final void logRoutingTableInformation(String cacheName) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Current consistent hash's routing table: %s", cacheName, currentCH.getRoutingTableAsString());
         if (pendingCH != null) log.tracef("[%s] Pending consistent hash's routing table: %s", cacheName, pendingCH.getRoutingTableAsString());
      }
   }

   /**
    * Phase of the rebalance process. Using four phases guarantees these properties:
    *
    * 1. T(x+1).writeCH contains all nodes from Tx.readCH (this is the requirement for ISPN-5021)
    * 2. Tx.readCH and T(x+1).readCH has non-empty subset of nodes (that will allow no blocking for read commands
    *    and reading only entries node owns according to readCH)
    *
    * Old entries should be wiped out only after coming to the {@link #NO_REBALANCE} phase.
    */
   @ProtoTypeId(ProtoStreamTypeIds.CACHE_TOPOLOGY_PHASE)
   public enum Phase {
      /**
       * Only currentCH should be set, this works as both readCH and writeCH
       */
      @ProtoEnumValue(1)
      NO_REBALANCE(false),

      /**
       * Interim state between NO_REBALANCE &rarr; READ_OLD_WRITE_ALL
       * readCh is set locally using previous Topology (of said node) readCH, whilst writeCH contains all members after merge
       */
      @ProtoEnumValue(2)
      CONFLICT_RESOLUTION(false),

      /**
       * Used during state transfer: readCH == currentCH, writeCH = unionCH
       */
      @ProtoEnumValue(3)
      READ_OLD_WRITE_ALL(true),

      /**
       * Used after state transfer completes: readCH == writeCH = unionCH
       */
      @ProtoEnumValue(4)
      READ_ALL_WRITE_ALL(false),

      /**
       * Intermediate state that prevents ISPN-5021: readCH == pendingCH, writeCH = unionCH
       */
      @ProtoEnumValue(5)
      READ_NEW_WRITE_ALL(false);

      private static final Phase[] values = Phase.values();
      private final boolean rebalance;


      Phase(boolean rebalance) {
         this.rebalance = rebalance;
      }

      public boolean isRebalance() {
         return rebalance;
      }

      public static Phase valueOf(int ordinal) {
         return values[ordinal];
      }
   }
}
