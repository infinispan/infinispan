package org.infinispan.partitionhandling.impl;

import static java.util.Arrays.asList;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;

/**
 * Mock {@link org.infinispan.topology.ClusterCacheStatus} for unit tests.
 *
 * It only maintains the current/stable topologies, verifying {@link AvailabilityStrategyContext} calls
 * requires a proper mock.
 *
 * @author Dan Berindei
 * @since 9.2
 */
class TestClusterCacheStatus {
   private final CacheJoinInfo joinInfo;

   private List<PersistentUUID> persistentUUIDs;
   private CacheTopology topology;
   private CacheTopology stableTopology;

   TestClusterCacheStatus(CacheJoinInfo joinInfo, List<PersistentUUID> persistentUUIDs, CacheTopology topology,
                          CacheTopology stableTopology) {
      this.joinInfo = joinInfo;
      this.persistentUUIDs = persistentUUIDs;
      this.topology = topology;
      assertNull(stableTopology.getPendingCH());
      this.stableTopology = stableTopology;
   }

   public static TestClusterCacheStatus start(CacheJoinInfo joinInfo, Address... members) {
      List<PersistentUUID> persistentUUIDs = Stream.of(members)
                                                   .map(TestClusterCacheStatus::persistentUUID)
                                                   .collect(Collectors.toList());
      ConsistentHash currentCH = joinInfo.getConsistentHashFactory()
                                         .create(joinInfo.getHashFunction(), joinInfo.getNumOwners(),
                                                 joinInfo.getNumSegments(), asList(members), null);
      CacheTopology topology = new CacheTopology(1, 1, currentCH, null, CacheTopology.Phase.NO_REBALANCE,
                                                 asList(members), persistentUUIDs);
      return new TestClusterCacheStatus(joinInfo, persistentUUIDs, topology, topology);
   }

   public TestClusterCacheStatus copy() {
      return new TestClusterCacheStatus(joinInfo, persistentUUIDs, topology, stableTopology);
   }

   public void startRebalance(CacheTopology.Phase phase, Address... targetMembers) {
      List<Address> targetList = asList(targetMembers);
      assertNull(topology.getPendingCH());
      ConsistentHash pendingCH = joinInfo.getConsistentHashFactory().updateMembers(topology.getCurrentCH(), targetList,
                                                                                   null);
      pendingCH = joinInfo.getConsistentHashFactory().rebalance(pendingCH);
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId() + 1, topology.getCurrentCH(),
                                   pendingCH, topology.getPhase(), targetList, persistentUUIDs);

   }

   public void advanceRebalance(CacheTopology.Phase phase) {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), topology.getCurrentCH(),
                                   topology.getPendingCH(), phase, topology.getActualMembers(), persistentUUIDs);
   }

   public void finishRebalance() {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), topology.getPendingCH(),
                                   null, CacheTopology.Phase.NO_REBALANCE,
                                   topology.getActualMembers(), persistentUUIDs);
   }

   public void cancelRebalance() {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), topology.getCurrentCH(),
                                   null, CacheTopology.Phase.NO_REBALANCE,
                                   topology.getActualMembers(), persistentUUIDs);
   }
   public void updateStableTopology() {
      assertEquals(CacheTopology.Phase.NO_REBALANCE, topology.getPhase());
      stableTopology = topology;
   }

   public void removeMember(Address leaver) {
      List<Address> updatedMembers = new ArrayList<>(topology.getActualMembers());
      updatedMembers.remove(leaver);
      ConsistentHash updatedCH = joinInfo.getConsistentHashFactory()
                                         .updateMembers(topology.getCurrentCH(), updatedMembers, null);
      ConsistentHash updatedPendingCH = topology.getPendingCH() != null ?
                                        joinInfo.getConsistentHashFactory()
                                                .updateMembers(topology.getPendingCH(), updatedMembers, null) :
                                        null;
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), updatedCH, updatedPendingCH,
                                   topology.getPhase(), updatedMembers, persistentUUIDs);
   }

   public ConsistentHash ch(Address... addresses) {
      return joinInfo.getConsistentHashFactory()
                     .create(joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(),
                             asList(addresses), null);
   }

   public static PersistentUUID persistentUUID(Address a) {
      return new PersistentUUID(a.hashCode(), a.hashCode());
   }

   public CacheJoinInfo joinInfo(Address a) {
      return joinInfo;
   }

   public CacheTopology topology() {
      return topology;
   }

   public CacheTopology stableTopology() {
      return stableTopology;
   }

   public void incrementIds(int topologyIdDelta, int rebalanceIdDelta) {
      topology = new CacheTopology(topology.getTopologyId() + topologyIdDelta,
                                   topology.getRebalanceId() + rebalanceIdDelta, topology.getCurrentCH(),
                                   topology.getPendingCH(), topology.getPhase(), topology.getActualMembers(),
                                   topology.getMembersPersistentUUIDs());
   }

   public void incrementStableIds(int topologyIdDelta, int rebalanceIdDelta) {
      stableTopology = new CacheTopology(stableTopology.getTopologyId() + topologyIdDelta,
                                         stableTopology.getRebalanceId() + rebalanceIdDelta,
                                         stableTopology.getCurrentCH(),
                                         stableTopology.getPendingCH(), stableTopology.getPhase(),
                                         stableTopology.getActualMembers(),
                                         stableTopology.getMembersPersistentUUIDs());
   }
}
