package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory.computeMembersWithoutState;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * Factory for ReplicatedConsistentHash.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class ReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {

   @Override
   public ReplicatedConsistentHash create(int numOwners, int numSegments, List<Address> members,
                                          Map<Address, Float> capacityFactors) {
      List<Address> membersWithoutState = computeMembersWithoutState(members, null, capacityFactors);
      int[] primaryOwners = new int[numSegments];
      int nextPrimaryOwner = 0;
      for (int i = 0; i < numSegments; i++) {
         // computeMembersWithoutState ensures that there is at least one member *with* state
         while (membersWithoutState.contains(members.get(nextPrimaryOwner))) {
            nextPrimaryOwner++;
            if (nextPrimaryOwner == members.size()) {
               nextPrimaryOwner = 0;
            }
         }
         primaryOwners[i] = nextPrimaryOwner;
      }
      return new ReplicatedConsistentHash(members, capacityFactors, membersWithoutState, primaryOwners);
   }

   @Override
   public ReplicatedConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!ReplicatedConsistentHash.class.getName().equals(consistentHashClass))
         throw CONTAINER.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new ReplicatedConsistentHash(state);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers,
                                                 Map<Address, Float> actualCapacityFactors) {
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      return updateCH(baseCH, newMembers, actualCapacityFactors, false);
   }

   private ReplicatedConsistentHash updateCH(ReplicatedConsistentHash baseCH, List<Address> newMembers,
                                             Map<Address, Float> actualCapacityFactors, boolean rebalance) {
      // New members missing from the old CH or with capacity factor 0 should not become primary or backup owners
      List<Address> membersWithoutState = computeMembersWithoutState(newMembers, baseCH.getMembers(),
                                                                     actualCapacityFactors);

      // recompute primary ownership based on the new list of members (removes leavers)
      int numSegments = baseCH.getNumSegments();
      int[] primaryOwners = new int[numSegments];
      int[] nodeUsage = new int[newMembers.size()];
      boolean foundOrphanSegments = false;
      for (int segmentId = 0; segmentId < numSegments; segmentId++) {
         Address primaryOwner = baseCH.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = newMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
         if (primaryOwnerIndex == -1) {
            foundOrphanSegments = true;
         } else {
            nodeUsage[primaryOwnerIndex]++;
         }
      }

      if (!foundOrphanSegments && !rebalance) {
         // The primary owners don't need to change
         return new ReplicatedConsistentHash(newMembers, actualCapacityFactors, membersWithoutState, primaryOwners);
      }

      // Exclude members without state by setting their usage to a very high value
      for (int i = 0; i < newMembers.size(); i++) {
         Address a = newMembers.get(i);
         if (membersWithoutState.contains(a)) {
            nodeUsage[i] = Integer.MAX_VALUE;
         }
      }

      // ensure leavers are replaced with existing members so no segments are orphan
      if (foundOrphanSegments) {
         for (int i = 0; i < numSegments; i++) {
            if (primaryOwners[i] == -1) {
               int leastUsed = findLeastUsedNode(nodeUsage);
               primaryOwners[i] = leastUsed;
               nodeUsage[leastUsed]++;
            }
         }
      }

      // ensure even spread of ownership
      int minSegmentsPerNode = numSegments / newMembers.size();
      Queue<Integer>[] segmentsByNode = new Queue[newMembers.size()];
      for (int segmentId = 0; segmentId < primaryOwners.length; ++segmentId) {
         int owner = primaryOwners[segmentId];
         Queue<Integer> segments = segmentsByNode[owner];
         if (segments == null) {
            segmentsByNode[owner] = segments = new ArrayDeque<>(minSegmentsPerNode);
         }
         segments.add(segmentId);
      }
      int mostUsedNode = 0;
      for (int node = 0; node < nodeUsage.length; node++) {
         while (nodeUsage[node] < minSegmentsPerNode) {
            // we can take segment from any node that has > minSegmentsPerNode + 1, not only the most used
            if (nodeUsage[mostUsedNode] <= minSegmentsPerNode + 1) {
               mostUsedNode = findMostUsedNode(nodeUsage);
            }
            int segmentId = segmentsByNode[mostUsedNode].poll();
            // we don't have to add the segmentId to the new owner's queue
            primaryOwners[segmentId] = node;
            nodeUsage[mostUsedNode]--;
            nodeUsage[node]++;
         }
      }

      return new ReplicatedConsistentHash(newMembers, actualCapacityFactors, membersWithoutState, primaryOwners);
   }

   private int findLeastUsedNode(int[] nodeUsage) {
      int res = 0;
      for (int node = 1; node < nodeUsage.length; node++) {
         if (nodeUsage[node] < nodeUsage[res]) {
            res = node;
         }
      }
      return res;
   }

   private int findMostUsedNode(int[] nodeUsage) {
      int res = 0;
      for (int node = 1; node < nodeUsage.length; node++) {
         if (nodeUsage[node] > nodeUsage[res]) {
            res = node;
         }
      }
      return res;
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return updateCH(baseCH, baseCH.getMembers(), baseCH.getCapacityFactors(), true);
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      return ch1.union(ch2);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return -6053;
   }

   public static class Externalizer extends AbstractExternalizer<ReplicatedConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, ReplicatedConsistentHashFactory chf) {
      }

      @Override
      public ReplicatedConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new ReplicatedConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.REPLICATED_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends ReplicatedConsistentHashFactory>> getTypeClasses() {
         return Util.asSet(ReplicatedConsistentHashFactory.class);
      }
   }
}
