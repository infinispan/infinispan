package org.infinispan.distribution.ch.impl;

import static java.lang.Math.min;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.topology.PersistentUUID;
import org.jgroups.util.UUID;

/**
 * {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation
 * that guarantees that multiple caches with the same members will
 * have the same consistent hash (unlike {@link DefaultConsistentHashFactory}).
 *
 * <p>It has a drawback compared to {@link DefaultConsistentHashFactory} though:
 * it can potentially move a lot more segments during a rebalance than
 * strictly necessary.
 * E.g. {0:AB, 1:BA, 2:CD, 3:DA} could turn into {0:BC, 1:CA, 2:CB, 3:AB} when D leaves,
 * even though {0:AB, 1:BA, 2:CB, 3:AC} would require fewer segment ownership changes.
 *
 * <p>It may also reorder the owners of a segments, e.g. AB -> BA
 * (same as {@linkplain DefaultConsistentHashFactory}).
 *
 * @author Dan Berindei
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SYNC_CONSISTENT_HASH)
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      checkCapacityFactors(members, capacityFactors);

      Builder builder = createBuilder(numOwners, numSegments, members, capacityFactors);
      builder.populateOwners();

      return new DefaultConsistentHash(numOwners, numSegments, members, capacityFactors, builder.segmentOwners);
   }

   @Override
   public DefaultConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!DefaultConsistentHash.class.getName().equals(consistentHashClass))
         throw CONTAINER.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new DefaultConsistentHash(state);
   }

   Builder createBuilder(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      return new Builder(numOwners, numSegments, members, capacityFactors);
   }

   void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors != null) {
         float totalCapacity = 0;
         for (Address node : members) {
            Float capacityFactor = capacityFactors.get(node);
            if (capacityFactor == null || capacityFactor < 0)
               throw new IllegalArgumentException("Invalid capacity factor for node " + node + ": " + capacityFactor);
            totalCapacity += capacityFactor;
         }
         if (totalCapacity == 0)
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
      }
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
                                              Map<Address, Float> actualCapacityFactors) {
      checkCapacityFactors(newMembers, actualCapacityFactors);

      // The ConsistentHashFactory contract says we should return the same instance if we're not making changes
      boolean sameCapacityFactors = actualCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            actualCapacityFactors.equals(baseCH.getCapacityFactors());
      if (newMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();

      // We assume leavers are far fewer than members, so it makes sense to check for leavers
      HashSet<Address> leavers = new HashSet<>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // Create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = null;

      // Remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = new ArrayList<>(baseCH.locateOwnersForSegment(s));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[s] = owners;
         } else {
            // this segment has 0 owners, fix it
            if (rebalancedCH == null) {
               rebalancedCH = create(numOwners, numSegments, newMembers, actualCapacityFactors);
            }
            newSegmentOwners[s] = rebalancedCH.locateOwnersForSegment(s);
         }
      }

      return new DefaultConsistentHash(numOwners, numSegments, newMembers,
            actualCapacityFactors, newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers(),
            baseCH.getCapacityFactors());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return -10007;
   }

   static class Builder {
      static final int NO_NODE = -1;

      // Input
      final int numOwners;
      final int numSegments;

      // Output
      final List<Address>[] segmentOwners;
      final int[][] ownerIndices;

      // Constant data
      final List<Address> sortedMembers;
      final int numNodes;
      final float[] sortedCapacityFactors;
      final float[] distanceFactors;
      final float totalCapacity;
      final int actualNumOwners;
      final int numNodeHashes;
      // Hashes use only 63 bits, or the interval 0..2^63-1
      final long segmentSize;
      final long[] segmentHashes;
      final long[][] nodeHashes;

      int nodeDistanceUpdates;
      final OwnershipStatistics stats;

      Builder(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.sortedMembers = sortMembersByCapacity(members, capacityFactors);
         this.sortedCapacityFactors = capacityFactorsToArray(sortedMembers, capacityFactors);
         this.totalCapacity = computeTotalCapacity();

         numNodes = sortedMembers.size();
         actualNumOwners = min(numOwners, numNodes);
         distanceFactors = capacityFactorsToDistanceFactors();
         segmentOwners = new List[numSegments];
         ownerIndices = new int[numSegments][];
         for (int s = 0; s < numSegments; s++) {
            segmentOwners[s] = new ArrayList<>(actualNumOwners);
            ownerIndices[s] = new int[actualNumOwners];
         }

         segmentSize = Long.MAX_VALUE / numSegments;
         segmentHashes = computeSegmentHashes(numSegments);
         // If we ever make the number of segments dynamic, the number of hashes should be fixed.
         // Otherwise the extra hashes would cause extra segment to move on segment number changes.
         numNodeHashes = 32 - Integer.numberOfLeadingZeros(numSegments);
         nodeHashes = computeNodeHashes();

         stats = new OwnershipStatistics(sortedMembers);
      }

      private float[] capacityFactorsToDistanceFactors() {
         // Nodes with capacity factor 0 have been removed
         float minCapacity = sortedCapacityFactors[numNodes - 1];
         float[] distanceFactors = new float[numNodes];
         for (int n = 0; n < numNodes; n++) {
            distanceFactors[n] = minCapacity / sortedCapacityFactors[n];
         }
         return distanceFactors;
      }

      private float[] capacityFactorsToArray(List<Address> sortedMembers, Map<Address, Float> capacityFactors) {
         float[] capacityFactorsArray = new float[sortedMembers.size()];
         for (int n = 0; n < sortedMembers.size(); n++) {
            capacityFactorsArray[n] = capacityFactors != null ? capacityFactors.get(sortedMembers.get(n)) : 1f;
         }
         return capacityFactorsArray;
      }

      private List<Address> sortMembersByCapacity(List<Address> members, Map<Address, Float> capacityFactors) {
         if (capacityFactors == null)
            return members;

         // Only add members with non-zero capacity
         List<Address> sortedMembers = new ArrayList<>();
         for (Address member : members) {
            if (!capacityFactors.get(member).equals(0f)) {
               sortedMembers.add(member);
            }
         }
         // Sort in descending order
         sortedMembers.sort((a1, a2) -> Float.compare(capacityFactors.get(a2), capacityFactors.get(a1)));
         return sortedMembers;
      }

      int[] computeExpectedSegments(int expectedOwners, float totalCapacity, int iteration) {
         int[] expected = new int[numNodes];
         float remainingCapacity = totalCapacity;
         int remainingCopies = expectedOwners * numSegments;
         float averageSegments = (float) remainingCopies / numNodes;
         for (int n = 0; n < numNodes; n++) {
            float capacityFactor = sortedCapacityFactors[n];
            if (capacityFactor == 0f) {
               expected[n] = 0;
            }

            float idealOwnedSegments = remainingCopies * capacityFactor / remainingCapacity;
            if (idealOwnedSegments > numSegments) {
               remainingCapacity -= capacityFactor;
               remainingCopies -= numSegments;
               expected[n] = numSegments;
            } else {
               // All the nodes from now on will have less than numSegments segments,
               // so we can stop updating remainingCapacity/remainingCopies
               expected[n] = fudgeExpectedSegments(idealOwnedSegments, averageSegments, iteration);
            }
         }
         return expected;
      }

      static int fudgeExpectedSegments(float idealOwnedSegments, float averageSegments, int iteration) {
         // In the first rounds reduce the number of expected segments so every node has a chance
         // In the later rounds increase the number of expected segments so every segment eventually finds an owner
         // It's harder to allocate the last segments to the nodes with large capacity,
         // so the step by which we reduce/increase is not linear with the number of ideal expected segments
         // But assign at least one extra segment per node every 5 iterations, in case there are too few segments
         float step = Math.max(Math.min(averageSegments * 0.05f, idealOwnedSegments * 0.15f), 1f);
         return Math.max((int) (idealOwnedSegments + (iteration - 2.5f ) * step), 0);
      }

      private long[] computeSegmentHashes(int numSegments) {
         assert segmentSize != 0;
         long[] segmentHashes = new long[numSegments];
         long currentSegmentHash = segmentSize >> 1;
         for (int s = 0; s < numSegments; s++) {
            segmentHashes[s] = currentSegmentHash;
            currentSegmentHash += segmentSize;
         }
         return segmentHashes;
      }

      private long[][] computeNodeHashes() {
         long[][] nodeHashes = new long[numNodes][];
         for (int n = 0; n < numNodes; n++) {
            nodeHashes[n] = new long[this.numNodeHashes];
            for (int h = 0; h < this.numNodeHashes; h++) {
               nodeHashes[n][h] = nodeHash(sortedMembers.get(n), h);
            }
            Arrays.sort(nodeHashes[n]);
         }
         return nodeHashes;
      }

      float computeTotalCapacity() {
         if (sortedCapacityFactors == null)
            return sortedMembers.size();

         float totalCapacity = 0;
         for (float sortedCapacityFactor : sortedCapacityFactors) {
            totalCapacity += sortedCapacityFactor;
         }
         return totalCapacity;
      }

      long nodeHash(Address address, int virtualNode) {
         // 64-bit hashes from 32-bit hashes have a non-negligible chance of collision,
         // so we try to get all 128 bits from UUID addresses
         long[] key = new long[2];
         if (address instanceof JGroupsAddress) {
            org.jgroups.Address jGroupsAddress = ((JGroupsAddress) address).getJGroupsAddress();
            if (jGroupsAddress instanceof UUID) {
               key[0] = ((UUID) jGroupsAddress).getLeastSignificantBits();
               key[1] = ((UUID) jGroupsAddress).getMostSignificantBits();
            } else {
               key[0] = address.hashCode();
            }
         } else if (address instanceof PersistentUUID) {
            key[0] = ((PersistentUUID) address).getLeastSignificantBits();
            key[1] = ((PersistentUUID) address).getMostSignificantBits();
         } else {
            key[0] = address.hashCode();
         }
         return MurmurHash3.MurmurHash3_x64_64(key, virtualNode) & Long.MAX_VALUE;
      }

      /**
       * @return distance between 2 points in the 0..2^63-1 range, max 2^62-1
       */
      long distance(long a, long b) {
         long distance = a < b ? b - a : a - b;
         if ((distance & (1L << 62)) != 0) {
            distance = -distance - Long.MIN_VALUE;
         }
         // For the -2^63..2^63-1 range, the code would be
         // if (distance < 0) {
         //    distance = -distance;
         // }
         return distance;
      }

      void populateOwners() {
         // List k contains each segment's kth closest available node
         PriorityQueue<SegmentInfo>[] segmentQueues = new PriorityQueue[Math.max(1, actualNumOwners)];
         for (int i = 0; i < segmentQueues.length; i++) {
            segmentQueues[i] = new PriorityQueue<>(numSegments);
         }
         // Temporary priority queue for one segment's potential owners
         PriorityQueue<SegmentInfo> temporaryQueue = new PriorityQueue<>(numNodes);

         assignSegments(1, totalCapacity, 1, segmentQueues, temporaryQueue);
         assert stats.sumPrimaryOwned() == numSegments;

         // The minimum queue count we can use is actualNumOwners - 1
         // A bigger queue count improves stability, i.e. a rebalance after a join/leave moves less segments around
         // However, the queueCount==1 case is optimized, so actualNumOwners-1 has better performance for numOwners=2
         assignSegments(actualNumOwners, totalCapacity, actualNumOwners - 1, segmentQueues, temporaryQueue);
         assert stats.sumOwned() == actualNumOwners * numSegments;
      }

      private void assignSegments(int currentNumOwners, float totalCapacity, int queuesCount,
                                  PriorityQueue<SegmentInfo>[] segmentQueues,
                                  PriorityQueue<SegmentInfo> temporaryQueue) {
         int totalCopies = currentNumOwners * numSegments;

         // We try to assign the closest node as the first owner, then the 2nd closest node etc.
         // But we also try to keep the number of owned segments per node close to the "ideal" number,
         // so we start by allocating a smaller number of segments to each node and slowly allow more segments.
         for (int loadIteration = 0; stats.sumOwned() < totalCopies; loadIteration++) {
            int[] nodeSegmentsToAdd = computeExpectedSegments(currentNumOwners, totalCapacity, loadIteration);
            int iterationCopies = 0;
            for (int n = 0; n < numNodes; n++) {
               iterationCopies += nodeSegmentsToAdd[n];
               nodeSegmentsToAdd[n] -= stats.getOwned(n);
            }

            iterationCopies = Math.max(iterationCopies, totalCopies);
            for (int distanceIteration = 0; distanceIteration < numNodes; distanceIteration++) {
               if (stats.sumOwned() >= iterationCopies)
                  break;

               populateQueues(currentNumOwners, nodeSegmentsToAdd, queuesCount, segmentQueues, temporaryQueue);

               if (!assignQueuedOwners(currentNumOwners, nodeSegmentsToAdd, queuesCount, iterationCopies, segmentQueues))
                  break;
            }
         }
      }

      // Useful for debugging
      private BitSet[] computeAvailableSegmentsPerNode(int currentNumOwners) {
         BitSet[] nodeSegmentsAvailable = new BitSet[numNodes];
         for (int s = 0; s < numSegments; s++) {
            if (!segmentIsAvailable(s, currentNumOwners))
               continue;

            for (int n = 0; n < numNodes; n++) {
               if (nodeCanOwnSegment(s, segmentOwners[s].size(), n)) {
                  if (nodeSegmentsAvailable[n] == null) {
                     nodeSegmentsAvailable[n] = new BitSet();
                  }
                  nodeSegmentsAvailable[n].set(s);
               }
            }
         }
         return nodeSegmentsAvailable;
      }

      private boolean assignQueuedOwners(int currentNumOwners, int[] nodeSegmentsToAdd, int queuesCount,
                                         int iterationCopies, PriorityQueue<SegmentInfo>[] segmentQueues) {
         boolean assigned = false;
         for (int i = 0; i < queuesCount; i++) {
            SegmentInfo si;
            while ((si = segmentQueues[i].poll()) != null) {
               int ownerPosition = segmentOwners[si.segment].size();
               if (nodeSegmentsToAdd[si.nodeIndex] <= 0)
                  continue;
               if (i == 0 ||
                   segmentIsAvailable(si.segment, currentNumOwners) &&
                   nodeCanOwnSegment(si.segment, ownerPosition, si.nodeIndex)) {
                  assignOwner(si.segment, ownerPosition, si.nodeIndex, nodeSegmentsToAdd);
                  assigned = true;
               }
               if (stats.sumOwned() >= iterationCopies) {
                  return assigned;
               }
            }
            segmentQueues[i].clear();
         }
         return assigned;
      }

      private void populateQueues(int currentNumOwners, int[] nodeSegmentsToAdd, int queueCount,
                                  PriorityQueue<SegmentInfo>[] segmentQueues,
                                  PriorityQueue<SegmentInfo> temporaryQueue) {
         // Bypass the temporary queue if the queue count is 1
         SegmentInfo best = null;
         for (int s = 0; s < numSegments; s++) {
            if (!segmentIsAvailable(s, currentNumOwners))
               continue;

            for (int n = 0; n < numNodes; n++) {
               if (nodeSegmentsToAdd[n] > 0 && nodeCanOwnSegment(s, segmentOwners[s].size(), n)) {
                  long scaledDistance = nodeSegmentDistance(n, segmentHashes[s]);
                  if (queueCount > 1) {
                     SegmentInfo si = new SegmentInfo(s, n, scaledDistance);
                     temporaryQueue.add(si);
                  } else {
                     if (best == null) {
                        best = new SegmentInfo(s, n, scaledDistance);
                     } else if (scaledDistance < best.distance) {
                        best.update(n, scaledDistance);
                     }
                  }
               }
            }

            if (queueCount > 1) {
               for (int i = 0; i < queueCount && !temporaryQueue.isEmpty(); i++) {
                  segmentQueues[i].add(temporaryQueue.remove());
               }
               temporaryQueue.clear();
            } else {
               if (best != null) {
                  segmentQueues[0].add(best);
               }
               best = null;
            }
         }
      }

      private boolean segmentIsAvailable(int segment, int currentNumOwners) {
         return segmentOwners[segment].size() < currentNumOwners;
      }

      private long nodeSegmentDistance(int nodeIndex, long segmentHash) {
         nodeDistanceUpdates++;
         long[] currentNodeHashes = nodeHashes[nodeIndex];
         int hashIndex = Arrays.binarySearch(currentNodeHashes, segmentHash);
         long scaledDistance;
         if (hashIndex > 0) {
            // Found an exact match
            scaledDistance = 0L;
         } else {
            // Flip to get the insertion point
            hashIndex = -(hashIndex + 1);

            long hashBefore = hashIndex > 0 ? currentNodeHashes[hashIndex - 1] : currentNodeHashes[numNodeHashes - 1];
            long hashAfter = hashIndex < numNodeHashes ? currentNodeHashes[hashIndex] : currentNodeHashes[0];

            long distance = min(distance(hashBefore, segmentHash), distance(hashAfter, segmentHash));
            scaledDistance = (long) (distance * distanceFactors[nodeIndex]);
         }
         return scaledDistance;
      }

      protected void assignOwner(int segment, int ownerPosition, int nodeIndex, int[] nodeSegmentsWanted) {
         assert nodeSegmentsWanted[nodeIndex] > 0;
         // One less segment needed for the assigned node
         --nodeSegmentsWanted[nodeIndex];

         assert segmentOwners[segment].size() == ownerPosition;
         segmentOwners[segment].add(sortedMembers.get(nodeIndex));
         ownerIndices[segment][ownerPosition] = nodeIndex;
         stats.incOwned(nodeIndex, ownerPosition == 0);
//         System.out.printf("owners[%d][%d] = %s (%d)\n", segment, ownerPosition, sortedMembers.get(nodeIndex), nodeIndex);
      }

      boolean nodeCanOwnSegment(int segment, int ownerPosition, int nodeIndex) {
         // Return false the node exists in the owners list
         return !intArrayContains(ownerIndices[segment], ownerPosition, nodeIndex);
      }

      boolean intArrayContains(int[] array, int end, int value) {
         for (int i = 0; i < end; i++) {
            if (array[i] == value)
               return true;
         }
         return false;
      }

      static class SegmentInfo implements Comparable<SegmentInfo> {
         static final int NO_AVAILABLE_OWNERS = -2;

         final int segment;
         int nodeIndex;
         long distance;

         SegmentInfo(int segment) {
            this.segment = segment;
            reset();
         }

         public SegmentInfo(int segment, int nodeIndex, long distance) {
            this.segment = segment;
            this.nodeIndex = nodeIndex;
            this.distance = distance;
         }

         void update(int closestNode, long minDistance) {
            this.nodeIndex = closestNode;
            this.distance = minDistance;
         }

         boolean isValid() {
            return nodeIndex >= 0;
         }

         void reset() {
            update(NO_NODE, Long.MAX_VALUE);
         }

         boolean hasNoAvailableOwners() {
            return nodeIndex == NO_AVAILABLE_OWNERS;
         }

         void markNoPotentialOwners() {
            update(NO_AVAILABLE_OWNERS, Long.MAX_VALUE);
         }

         @Override
         public int compareTo(SegmentInfo o) {
            // Sort ascending by distance
            return Long.compare(distance, o.distance);
         }

         @Override
         public String toString() {
            if (nodeIndex >= 0) {
               return String.format("SegmentInfo#%d{n=%d, distance=%016x}", segment, nodeIndex, distance);
            }
            return String.format("SegmentInfo#%d{%s}", segment, segmentDescription());
         }

         private String segmentDescription() {
            switch (nodeIndex) {
               case NO_NODE:
                  return "NO_NODE";
               case NO_AVAILABLE_OWNERS:
                  return "NO_AVAILABLE_OWNERS";
               default:
                  return String.valueOf(segment);
            }
         }
      }
   }
}
