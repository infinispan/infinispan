package org.infinispan.topology;

import static java.util.Arrays.asList;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.partitionhandling.impl.AvailabilityStrategy;
import org.infinispan.partitionhandling.impl.AvailabilityStrategyContext;
import org.infinispan.remoting.transport.Address;

/**
 * Mock {@link org.infinispan.topology.ClusterCacheStatus} for unit tests.
 *
 * It only maintains the current/stable topologies, verifying {@link AvailabilityStrategyContext} calls
 * requires a proper mock.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class TestClusterCacheStatus {
   private final CacheJoinInfo joinInfo;

   private CacheTopology topology;
   private CacheTopology stableTopology;

   public TestClusterCacheStatus(CacheJoinInfo joinInfo, CacheTopology topology, CacheTopology stableTopology) {
      this.joinInfo = joinInfo;
      this.topology = topology;
      assertNull(stableTopology.getPendingCH());
      this.stableTopology = stableTopology;
   }

   public static TestClusterCacheStatus start(CacheJoinInfo joinInfo, Address... members) {
      List<Address> membersList = asList(members);
      return start(joinInfo, membersList);
   }

   public static TestClusterCacheStatus start(CacheJoinInfo joinInfo, List<Address> members) {
      ConsistentHash currentCH = joinInfo.getConsistentHashFactory()
                                         .create(joinInfo.getNumOwners(),
                                                 joinInfo.getNumSegments(), members, null);
      CacheTopology topology = new CacheTopology(1, 1, currentCH, null, null, CacheTopology.Phase.NO_REBALANCE, members,
                                                 persistentUUIDs(members));
      return new TestClusterCacheStatus(joinInfo, topology, topology);
   }

   public TestClusterCacheStatus copy() {
      return new TestClusterCacheStatus(joinInfo, topology, stableTopology);
   }

   public void startRebalance(CacheTopology.Phase phase, Address... targetMembers) {
      startRebalance(phase, asList(targetMembers));
   }

   public void startRebalance(CacheTopology.Phase phase, List<Address> targetMembers) {
      assertNull(topology.getPendingCH());
      assertTrue(targetMembers.containsAll(topology.getCurrentCH().getMembers()));
      ConsistentHash pendingCH = joinInfo.getConsistentHashFactory().updateMembers(topology.getCurrentCH(), targetMembers,
                                                                                   null);
      pendingCH = joinInfo.getConsistentHashFactory().rebalance(pendingCH);
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId() + 1, topology.getCurrentCH(),
                                   pendingCH, null, phase, targetMembers, persistentUUIDs(targetMembers));
   }

   public void advanceRebalance(CacheTopology.Phase phase) {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), topology.getCurrentCH(),
                                   topology.getPendingCH(), topology.getUnionCH(), phase, topology.getActualMembers(),
                                   persistentUUIDs(topology.getMembers()));
   }

   public void finishRebalance() {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), topology.getPendingCH(),
                                   null, null, CacheTopology.Phase.NO_REBALANCE,
                                   topology.getActualMembers(), persistentUUIDs(topology.getActualMembers()));
   }

   public void cancelRebalance() {
      assertNotSame(CacheTopology.Phase.NO_REBALANCE, topology.getPhase());
      assertNotSame(CacheTopology.Phase.CONFLICT_RESOLUTION, topology.getPhase());
      // Use the read CH as the current CH
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId() + 1,
                                   readConsistentHash(), null, null, CacheTopology.Phase.NO_REBALANCE,
                                   topology.getActualMembers(), persistentUUIDs(topology.getActualMembers()));
   }

   /**
    * The topologies included in the status responses do not have a union CH, so
    * {@link CacheTopology#getReadConsistentHash()} doesn't work.
    */
   public ConsistentHash readConsistentHash() {
      return AvailabilityStrategy.ownersConsistentHash(topology, joinInfo.getConsistentHashFactory());
   }

   public void updateStableTopology() {
      assertEquals(CacheTopology.Phase.NO_REBALANCE, topology.getPhase());
      stableTopology = topology;
   }

   public void removeMembers(Address... leavers) {
      removeMembers(asList(leavers));
   }

   public void removeMembers(List<Address> leavers) {
      List<Address> updatedMembers = new ArrayList<>(topology.getActualMembers());
      updatedMembers.removeAll(leavers);
      assertEquals(topology.getActualMembers().size(), leavers.size() + updatedMembers.size());
      ConsistentHash updatedCH = joinInfo.getConsistentHashFactory()
                                         .updateMembers(topology.getCurrentCH(), updatedMembers, null);
      ConsistentHash updatedPendingCH = topology.getPendingCH() != null ?
                                        joinInfo.getConsistentHashFactory()
                                                .updateMembers(topology.getPendingCH(), updatedMembers, null) :
                                        null;
      ConsistentHash updatedUnionCH = topology.getUnionCH() != null ?
                                      joinInfo.getConsistentHashFactory()
                                              .updateMembers(topology.getUnionCH(), updatedMembers, null) :
                                      null;
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId(), updatedCH, updatedPendingCH,
                                   updatedUnionCH, topology.getPhase(), updatedMembers,
                                   persistentUUIDs(updatedMembers));
   }

   public void startConflictResolution(ConsistentHash conflictCH, Address... mergeMembers) {
      startConflictResolution(conflictCH, asList(mergeMembers));
   }

   public void startConflictResolution(ConsistentHash conflictCH, List<Address> mergeMembers) {
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId() + 1,
                                   conflictCH, null, CacheTopology.Phase.CONFLICT_RESOLUTION, mergeMembers,
                                   persistentUUIDs(mergeMembers));
   }

   public static ConsistentHash conflictResolutionConsistentHash(TestClusterCacheStatus... caches) {
      ConsistentHashFactory chf = caches[0].joinInfo.getConsistentHashFactory();
      ConsistentHash hash = Stream.of(caches)
                                  .map(TestClusterCacheStatus::readConsistentHash)
                                  .reduce(chf::union)
                                  .orElseThrow(IllegalStateException::new);
      return chf.union(hash, chf.rebalance(hash));
   }

   public ConsistentHash ch(Address... addresses) {
      return joinInfo.getConsistentHashFactory()
                     .create(joinInfo.getNumOwners(), joinInfo.getNumSegments(),
                             asList(addresses), null);
   }

   public static UUID persistentUUID(Address a) {
      return new UUID(a.hashCode(), a.hashCode());
   }

   private static List<UUID> persistentUUIDs(List<Address> members) {
      return members.stream()
                    .map(TestClusterCacheStatus::persistentUUID)
                    .collect(Collectors.toList());
   }

   public CacheJoinInfo joinInfo(Address a) {
      // Copy the generic CacheJoinInfo and replace the persistent UUID
      return new CacheJoinInfo(joinInfo.getConsistentHashFactory(), joinInfo.getNumSegments(), joinInfo.getNumOwners(),
            joinInfo.getTimeout(), joinInfo.getCacheMode(), joinInfo.getCapacityFactor(),
            persistentUUID(a), joinInfo.getPersistentStateChecksum());
   }

   public CacheTopology topology() {
      return topology;
   }

   public CacheTopology stableTopology() {
      return stableTopology;
   }

   public void incrementIds(int topologyIdDelta, int rebalanceIdDelta) {
      topology = new CacheTopology(topology.getTopologyId() + topologyIdDelta,
                                   topology.getRebalanceId() + rebalanceIdDelta,
                                   topology.getCurrentCH(), topology.getPendingCH(), topology.getUnionCH(),
                                   topology.getPhase(), topology.getActualMembers(),
                                   topology.getMembersPersistentUUIDs());
   }

   public void incrementStableIds(int topologyIdDelta, int rebalanceIdDelta) {
      assertSame(CacheTopology.Phase.NO_REBALANCE, stableTopology.getPhase());
      assertNull(stableTopology.getPendingCH());
      assertNull(stableTopology.getUnionCH());
      stableTopology = new CacheTopology(stableTopology.getTopologyId() + topologyIdDelta,
                                         stableTopology.getRebalanceId() + rebalanceIdDelta,
                                         stableTopology.getCurrentCH(), null, null,
                                         stableTopology.getPhase(), stableTopology.getActualMembers(),
                                         stableTopology.getMembersPersistentUUIDs());
   }

   public void incrementIds() {
      incrementIds(1, 1);
   }

   public void incrementIdsIfNeeded(TestClusterCacheStatus... otherPartitions) {
      // If there is any other partition with the same topology id, use that topology id + 1
      // Same with the rebalance id
      int newTopologyId = topology.getTopologyId();
      int newRebalanceId = topology.getRebalanceId();
      for (TestClusterCacheStatus cache : otherPartitions) {
         newTopologyId = Math.max(cache.topology.getTopologyId() + 1, newTopologyId);
         newRebalanceId = Math.max(cache.topology.getRebalanceId() + 1, newRebalanceId);
      }
      topology = new CacheTopology(newTopologyId, newRebalanceId,
                                   topology.getCurrentCH(), topology.getPendingCH(), topology.getUnionCH(),
                                   topology.getPhase(), topology.getActualMembers(),
                                   topology.getMembersPersistentUUIDs());
   }

   public void updateActualMembers(Address... actualMembers) {
      updateActualMembers(asList(actualMembers));
   }

   public void updateActualMembers(List<Address> actualMembers) {
      assertTrue(topology.getMembers().containsAll(actualMembers));
      topology = new CacheTopology(topology.getTopologyId() + 1, topology.getRebalanceId() + 1,
                                   topology.getCurrentCH(), topology.getPendingCH(), topology.getUnionCH(),
                                   topology.getPhase(), actualMembers, persistentUUIDs(actualMembers));
   }
}
