package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class AbstractConsistentHashFactory<CH extends ConsistentHash> implements ConsistentHashFactory<CH> {
   protected void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors != null) {
         float totalCapacity = 0;
         for (Address node : members) {
            Float capacityFactor = capacityFactors.get(node);
            if (capacityFactor == null || capacityFactor < 0)
               throw new IllegalArgumentException("Invalid capacity factor for node " + node);
            totalCapacity += capacityFactor;
         }
         if (totalCapacity == 0)
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
      }
   }

   /**
    * @return The worst backup owner, or {@code null} if the remaining nodes own 0 segments.
    */
   protected Address findWorstPrimaryOwner(Builder builder, List<Address> nodes) {
      Address worst = null;
      float maxSegmentsPerCapacity = -1;
      for (Address owner : nodes) {
         float capacityFactor = builder.getCapacityFactor(owner);
         if (builder.getPrimaryOwned(owner) - 1 >= capacityFactor * maxSegmentsPerCapacity) {
            worst = owner;
            maxSegmentsPerCapacity = capacityFactor != 0 ? (builder.getPrimaryOwned(owner) - 1) / capacityFactor : 0;
         }
      }
      return worst;
   }

   /**
    * @return The candidate with the worst primary-owned segments/capacity ratio that is also not in the excludes list.
    */
   protected Address findNewPrimaryOwner(Builder builder, Collection<Address> candidates,
                                         Address primaryOwner) {
      float initialCapacityFactor = primaryOwner != null ? builder.getCapacityFactor(primaryOwner) : 0;

      // We want the owned/capacity ratio of the actual primary owner after removing the current segment to be bigger
      // than the owned/capacity ratio of the new primary owner after adding the current segment, so that a future pass
      // won't try to switch them back.
      Address best = null;
      float bestSegmentsPerCapacity = initialCapacityFactor != 0 ? (builder.getPrimaryOwned(primaryOwner) - 1) /
            initialCapacityFactor : Float.MAX_VALUE;
      for (Address candidate : candidates) {
         int primaryOwned = builder.getPrimaryOwned(candidate);
         float capacityFactor = builder.getCapacityFactor(candidate);
         if ((primaryOwned + 1) <= capacityFactor * bestSegmentsPerCapacity) {
            best = candidate;
            bestSegmentsPerCapacity = (primaryOwned + 1) / capacityFactor;
         }
      }
      return best;
   }

   static abstract class Builder {
      protected final Hash hashFunction;
      protected final OwnershipStatistics stats;
      protected final List<Address> members;
      protected final Map<Address, Float> capacityFactors;
      // For debugging
      protected int modCount = 0;

      public Builder(Hash hashFunction, OwnershipStatistics stats, List<Address> members, Map<Address, Float> capacityFactors) {
         this.hashFunction = hashFunction;
         this.stats = stats;
         this.members = members;
         this.capacityFactors = capacityFactors;
      }

      public Builder(Builder other) {
         this.hashFunction = other.hashFunction;
         this.members = other.members;
         this.capacityFactors = other.capacityFactors;
         this.stats = new OwnershipStatistics(other.stats);
      }

      public List<Address> getMembers() {
         return members;
      }

      public int getNumNodes() {
         return getMembers().size();
      }

      public abstract int getPrimaryOwned(Address candidate);

      public Map<Address, Float> getCapacityFactors() {
         return capacityFactors;
      }

      public float getCapacityFactor(Address node) {
         return capacityFactors != null ? capacityFactors.get(node) : 1;
      }

      public float getTotalCapacity() {
         if (capacityFactors == null)
            return getNumNodes();

         float totalCapacity = 0;
         for (Address node : members) {
            totalCapacity += capacityFactors.get(node);
         }
         return totalCapacity;
      }
   }
}
