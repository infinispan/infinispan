package org.infinispan.distribution.ch.impl;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.distribution.Member;
import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation that guarantees caches
 * with the same members have the same consistent hash and also tries to distribute segments based on the
 * topology information in {@link org.infinispan.configuration.global.TransportConfiguration}.
 * <p/>
 * It has a drawback compared to {@link org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory}:
 * it can potentially move a lot more segments during a rebalance than strictly necessary.
 * <p/>
 * It is not recommended using the {@code TopologyAwareSyncConsistentHashFactory} with a very small number
 * of segments. The distribution of segments to owners gets better with a higher number of segments, and is
 * especially bad when {@code numSegments &lt; numNodes}
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyAwareSyncConsistentHashFactory extends SyncConsistentHashFactory {
   @Override
   protected Builder createBuilder(int numOwners, int numSegments, List<Member> members) {
      return new Builder(numOwners, numSegments, members);
   }

   protected static class Builder extends SyncConsistentHashFactory.Builder {
      final Map<Address, Float> capacityFactorsMap;
      protected final TopologyInfo topologyInfo;
      private final List<Address> addresses;
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

      protected Builder(int numOwners, int numSegments, List<Member> members) {
         super(numOwners, numSegments, members);

         addresses = sortedMembers.stream().map(Member::address).collect(Collectors.toList());
         capacityFactorsMap = members.stream().collect(Collectors.toMap(Member::address, Member::capacityFactor));
         topologyInfo = new TopologyInfo(numSegments, this.actualNumOwners, addresses, capacityFactorsMap);

         numSites = topologyInfo.getDistinctLocationsCount(TopologyLevel.SITE);
         numRacks = topologyInfo.getDistinctLocationsCount(TopologyLevel.RACK);
         numMachines = topologyInfo.getDistinctLocationsCount(TopologyLevel.MACHINE);
         siteLookup = new int[numNodes];
         rackLookup = new int[numNodes];
         machineLookup = new int[numNodes];
         for (int n = 0; n < numNodes; n++) {
            Address address = addresses.get(n);
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
      protected int[] computeExpectedSegments(int expectedOwners, float totalCapacity, int iteration) {
         TopologyInfo topologyInfo = new TopologyInfo(numSegments, expectedOwners, addresses, capacityFactorsMap);
         int[] expectedSegments = new int[numNodes];
         float averageSegments = (float) numSegments * expectedOwners / numNodes;
         for (int n = 0; n < numNodes; n++) {
            float idealOwnedSegments = topologyInfo.getExpectedOwnedSegments(addresses.get(n));
            expectedSegments[n] = fudgeExpectedSegments(idealOwnedSegments, averageSegments, iteration);
         }
         return expectedSegments;
      }

      @Override
      protected boolean nodeCanOwnSegment(int segment, int ownerPosition, int nodeIndex) {
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

   public static class Externalizer extends AbstractExternalizer<TopologyAwareSyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, TopologyAwareSyncConsistentHashFactory chf) {
      }

      @Override
      public TopologyAwareSyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new TopologyAwareSyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.TOPOLOGY_AWARE_SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends TopologyAwareSyncConsistentHashFactory>> getTypeClasses() {
         return Collections.singleton(TopologyAwareSyncConsistentHashFactory.class);
      }
   }
}
