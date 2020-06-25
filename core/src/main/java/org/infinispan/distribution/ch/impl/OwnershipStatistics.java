package org.infinispan.distribution.ch.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

/**
 * This class holds statistics about a consistent hash. It counts how many segments are owned or primary-owned by each
 * member.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class OwnershipStatistics {
   private final List<Address> nodes;
   private final Map<Address, Integer> nodesMap;
   private final int[] primaryOwned;
   private final int[] owned;
   private int sumPrimary;
   private int sumOwned;

   public OwnershipStatistics(List<Address> nodes) {
      this.nodes = nodes;
      this.nodesMap = new HashMap<>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
         this.nodesMap.put(nodes.get(i), i);
      }
      if (this.nodesMap.size() != nodes.size()) {
         throw new IllegalArgumentException("Nodes are not distinct: " + nodes);
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
            Integer nodeIndex = nodesMap.get(address);
            if (nodeIndex != null) {
               if (j == 0) {
                  primaryOwned[nodeIndex]++;
                  sumPrimary++;
               }
               owned[nodeIndex]++;
               sumOwned++;
            }
         }
      }
   }

   public OwnershipStatistics(ConsistentHash ch) {
      this(ch, ch.getMembers());
   }

   public OwnershipStatistics(OwnershipStatistics other) {
      this.nodes = other.nodes;
      this.nodesMap = other.nodesMap;
      this.primaryOwned = Arrays.copyOf(other.primaryOwned, other.primaryOwned.length);
      this.owned = Arrays.copyOf(other.owned, other.owned.length);
      this.sumPrimary = other.sumPrimary;
      this.sumOwned = other.sumOwned;
   }


   public int getPrimaryOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         return 0;
      return primaryOwned[i];
   }

   public int getOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         return 0;
      return owned[i];
   }

   public void incPrimaryOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]++;
      sumPrimary++;
   }

   public void incOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]++;
      sumOwned++;
   }

   public void decPrimaryOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]--;
      sumPrimary--;
   }

   public void decOwned(Address a) {
      Integer i = nodesMap.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]--;
      sumOwned--;
   }

   public int getPrimaryOwned(int nodeIndex) {
      return primaryOwned[nodeIndex];
   }

   public int getOwned(int nodeIndex) {
      return owned[nodeIndex];
   }

   public void incPrimaryOwned(int nodeIndex) {
      primaryOwned[nodeIndex]++;
      sumPrimary++;
   }

   public void incOwned(int nodeIndex) {
      owned[nodeIndex]++;
      sumOwned++;
   }

   public void incOwned(int nodeIndex, boolean primary) {
      owned[nodeIndex]++;
      sumOwned++;
      if (primary) {
         incPrimaryOwned(nodeIndex);
      }
   }

   public void decPrimaryOwned(int nodeIndex) {
      primaryOwned[nodeIndex]--;
      sumPrimary--;
   }

   public void decOwned(int nodeIndex) {
      owned[nodeIndex]--;
      sumOwned--;
   }

   public int sumPrimaryOwned() {
      return sumPrimary;
   }

   public int sumOwned() {
      return sumOwned;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("OwnershipStatistics{");
      boolean isFirst = true;
      for (Address node : nodes) {
         if (!isFirst) {
            sb.append(", ");
         }
         Integer index = nodesMap.get(node);
         sb.append(node).append(": ")
           .append(owned[index]).append('(')
           .append(primaryOwned[index]).append("p)");
         isFirst = false;
      }
      sb.append('}');
      return sb.toString();
   }
}
