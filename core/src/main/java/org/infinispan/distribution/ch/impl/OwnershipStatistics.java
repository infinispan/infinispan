package org.infinispan.distribution.ch.impl;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds statistics about a consistent hash. It counts how many segments are owned or primary-owned by each
 * member.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class OwnershipStatistics {
   private final Map<Address, Integer> nodes;
   private final int[] primaryOwned;
   private final int[] owned;

   public OwnershipStatistics(List<Address> nodes) {
      this.nodes = new HashMap<Address, Integer>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
         this.nodes.put(nodes.get(i), i);
      }
      this.primaryOwned = new int[nodes.size()];
      this.owned = new int[nodes.size()];
   }

   public OwnershipStatistics(ConsistentHash ch, List<Address> activeNodes) {
      this(activeNodes);

      for (int i = 0; i < ch.getNumSegments(); i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         for (int j = 0; j < owners.size(); j++) {
            Address address = owners.get(j);
            Integer nodeIndex = nodes.get(address);
            if (nodeIndex != null) {
               if (j == 0) {
                  primaryOwned[nodeIndex]++;
               }
               owned[nodeIndex]++;
            }
         }
      }
   }

   public OwnershipStatistics(OwnershipStatistics other) {
      this.nodes = new HashMap<Address, Integer>(other.nodes);
      this.primaryOwned = Arrays.copyOf(other.primaryOwned, other.primaryOwned.length);
      this.owned = Arrays.copyOf(other.owned, other.owned.length);
   }


   public int getPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         return 0;
      return primaryOwned[i];
   }

   public int getOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         return 0;
      return owned[i];
   }

   public void incPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]++;
   }

   public void incOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]++;
   }

   public void decPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]--;
   }

   public void decOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]--;
   }

   public int sumOwned() {
      int allOwnersCount = 0;
      for (int ownedCount : owned) {
         allOwnersCount += ownedCount;
      }
      return allOwnersCount;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("OwnershipStatistics{");
      boolean isFirst = true;
      for (Map.Entry<Address, Integer> e : nodes.entrySet()) {
         if (!isFirst) {
            sb.append(", ");
         }
         Address node = e.getKey();
         Integer index = e.getValue();
         sb.append(node).append(": ").append(primaryOwned[index]).append('+').append(owned[index] - primaryOwned[index]);
         isFirst = false;
      }
      return sb.toString();
   }
}
