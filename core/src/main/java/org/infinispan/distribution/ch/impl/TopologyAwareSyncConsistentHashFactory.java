package org.infinispan.distribution.ch.impl;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation that guarantees caches
 * with the same members have the same consistent hash and also tries to distribute segments based on the
 * topology information in {@link org.infinispan.configuration.global.TransportConfiguration}.
  * It has a drawback compared to {@link org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory}:
 * it can potentially move a lot more segments during a rebalance than strictly necessary.
  * It is not recommended using the {@code TopologyAwareSyncConsistentHashFactory} with a very small number
 * of segments. The distribution of segments to owners gets better with a higher number of segments, and is
 * especially bad when {@code numSegments &lt; numNodes}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.TOPOLOGY_AWARE_SYNC_CONSISTENT_HASH)
public class TopologyAwareSyncConsistentHashFactory extends SyncConsistentHashFactory {
   @Override
   protected Builder createBuilder(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      return new Builder(numOwners, numSegments, members, capacityFactors);
   }

   protected static class Builder extends SyncConsistentHashFactory.Builder {
      final Map<Address, Float> capacityFactorsMap;
      protected final TopologyInfo topologyInfo;
      // Speed up the site/rack/machine checks by mapping each to an integer
      // and comparing only integers in nodeCanOwnSegment()
      final int numSites;
      final int numRacks;
      final int numMachines;
      final int[] siteLookup;
      final int[] rackLookup;
      final int[] machineLookup;
      final int[][] ownerSiteIndices;
      final int[][] ownerRackIndices;
      final int[][] ownerMachineIndices;

      protected Builder(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
         super(numOwners, numSegments, members, capacityFactors);

         capacityFactorsMap = capacityFactors;
         topologyInfo = new TopologyInfo(numSegments, this.actualNumOwners, members, capacityFactors);

         numSites = topologyInfo.getDistinctLocationsCount(TopologyLevel.SITE);
         numRacks = topologyInfo.getDistinctLocationsCount(TopologyLevel.RACK);
         numMachines = topologyInfo.getDistinctLocationsCount(TopologyLevel.MACHINE);
         siteLookup = new int[numNodes];
         rackLookup = new int[numNodes];
         machineLookup = new int[numNodes];
         for (int n = 0; n < numNodes; n++) {
            Address address = sortedMembers.get(n);
            siteLookup[n] = topologyInfo.getSiteIndex(address);
            rackLookup[n] = topologyInfo.getRackIndex(address);
            machineLookup[n] = topologyInfo.getMachineIndex(address);
         }
         ownerSiteIndices = new int[numSegments][];
         ownerRackIndices = new int[numSegments][];
         ownerMachineIndices = new int[numSegments][];
         for (int s = 0; s < numSegments; s++) {
            ownerSiteIndices[s] = new int[actualNumOwners];
            ownerRackIndices[s] = new int[actualNumOwners];
            ownerMachineIndices[s] = new int[actualNumOwners];
         }
      }

      @Override
      int[] computeExpectedSegments(int expectedOwners, float totalCapacity, int iteration) {
         TopologyInfo topologyInfo = new TopologyInfo(numSegments, expectedOwners, sortedMembers, capacityFactorsMap);
         int[] expectedSegments = new int[numNodes];
         float averageSegments = (float) numSegments * expectedOwners / numNodes;
         for (int n = 0; n < numNodes; n++) {
            float idealOwnedSegments = topologyInfo.getExpectedOwnedSegments(sortedMembers.get(n));
            expectedSegments[n] = fudgeExpectedSegments(idealOwnedSegments, averageSegments, iteration);
         }
         return expectedSegments;
      }

      @Override
      boolean nodeCanOwnSegment(int segment, int ownerPosition, int nodeIndex) {
         if (ownerPosition == 0) {
            return true;
         } else if (ownerPosition < numSites) {
            // Must be different site
            return !intArrayContains(ownerSiteIndices[segment], ownerPosition, siteLookup[nodeIndex]);
         } else if (ownerPosition < numRacks) {
            // Must be different rack
            return !intArrayContains(ownerRackIndices[segment], ownerPosition, rackLookup[nodeIndex]);
         } else if (ownerPosition < numMachines) {
            // Must be different machine
            return !intArrayContains(ownerMachineIndices[segment], ownerPosition, machineLookup[nodeIndex]);
         } else {
            // Must be different nodes
            return !intArrayContains(ownerIndices[segment], ownerPosition, nodeIndex);
         }
      }

      @Override
      protected void assignOwner(int segment, int ownerPosition, int nodeIndex,
                                 int[] nodeSegmentsWanted) {
         super.assignOwner(segment, ownerPosition, nodeIndex, nodeSegmentsWanted);

         ownerSiteIndices[segment][ownerPosition] = siteLookup[nodeIndex];
         ownerRackIndices[segment][ownerPosition] = rackLookup[nodeIndex];
         ownerMachineIndices[segment][ownerPosition] = machineLookup[nodeIndex];
      }
   }
}
