package org.infinispan.distribution.ch;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link ConsistentHashFactory} implementation that guarantees caches with the same members
 * have the same consistent hash.
 *
 * It has a drawback compared to {@link DefaultConsistentHashFactory}, though: it can potentially
 * move a lot more segments during a rebalance than strictly necessary.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      checkCapacityFactors(members, capacityFactors);

      Builder builder = new Builder(hashFunction, numOwners, numSegments, members, capacityFactors);
      SortedMap<Integer, Address> primarySegments = populatePrimarySegments(builder, numSegments);
      if (numSegments >= builder.nodesWithLoad()) {
         populateOwnersManySegments(builder, primarySegments);
      } else {
         populateOwnersFewSegments(builder, primarySegments);
      }

      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, capacityFactors, builder.getAllOwners());
   }

   private void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
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

   protected void populateOwnersFewSegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Too few segments for each member to have one "primary segment",
      // but we may still have enough segments for each member to be a backup owner.
      // Populate the primary owners first - because numSegments < numMembers we're guaranteed to
      // set the primary owner of each segment
      int actualNumOwners = builder.getActualNumOwners();
      TreeSet<Address> sortedMembers = new TreeSet<Address>(builder.getSortedMembers());
      for (Map.Entry<Integer, Address> e : primarySegments.entrySet()) {
         Integer segment = e.getKey();
         Address primaryOwner = e.getValue();
         List<Address> owners = builder.getOwners(segment);
         owners.add(primaryOwner);
         if (owners.size() >= actualNumOwners)
            continue;

         for (Address a : sortedMembers.tailSet(primaryOwner, false)) {
            if (owners.size() >= actualNumOwners)
               break;
            if (builder.getCapacityFactor(a) > 0 && !owners.contains(a)) {
               owners.add(a);
            }
         }
         for (Address a : sortedMembers.headSet(primaryOwner, false)) {
            if (owners.size() >= actualNumOwners)
               break;
            if (builder.getCapacityFactor(a) > 0 && !owners.contains(a)) {
               owners.add(a);
            }
         }
      }
   }

   protected int normalizedHash(Hash hashFunction, int hashcode) {
      return hashFunction.hash(hashcode) & Integer.MAX_VALUE;
   }

   protected void populateOwnersManySegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Each member is present at least once in the primary segments map, so we can use that
      // to populate the owner lists. For each segment assign the owners of the next numOwners
      // "primary segments" as owners.
      int actualNumOwners = builder.getActualNumOwners();
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         for (Address a : primarySegments.tailMap(segment).values()) {
            if (owners.size() >= actualNumOwners)
               break;
            if (!owners.contains(a)) {
               owners.add(a);
            }
         }
         if (owners.size() < actualNumOwners) {
            for (Address a : primarySegments.headMap(segment).values()) {
               if (owners.size() >= actualNumOwners)
                  break;
               if (!owners.contains(a)) {
                  owners.add(a);
               }
            }
         }
      }
   }

   /**
    * Finds a unique "primary segment" for each virtual member
    */
   private SortedMap<Integer, Address> populatePrimarySegments(Builder builder, int numSegments) {
      // Only used for debugging
      int collisions = 0;

      List<Address> sortedMembers = builder.getSortedMembers();
      int nodesWithLoad = builder.nodesWithLoad();
      int numNodes = sortedMembers.size();

      float maxCapacityFactor = 1;
      float totalCapacity = 0;
      for (Address member : sortedMembers) {
         Float capacityFactor = builder.getCapacityFactor(member);
         if (capacityFactor > maxCapacityFactor) {
            maxCapacityFactor = capacityFactor;
         }
         totalCapacity += capacityFactor;
      }

      // Since the number of segments is potentially much larger than the number of members,
      // we need a concept of "virtual nodes" to help split the segments more evenly.
      // However, we don't have a "numVirtualNodes" setting any more, so we try to guess it
      // based on numSegments. This is not perfect because we may end up with too many virtual nodes,
      // but the only downside in that is a little more shuffling when a node joins/leaves.
      double totalVirtualNodes = nodesWithLoad * Math.sqrt(numSegments);
      // Determine how many virtual nodes each node has based on its capacity factor compared to the maximum capacity factor
      // (which has numVirtualNodes virtual nodes).
      Map<Address, Integer> virtualNodeCounts = new HashMap<Address, Integer>(numNodes);
      for (Address member : sortedMembers) {
         Float capacityFactor = builder.getCapacityFactor(member);
         int vn = 0;
         // Every node should have at least one virtual node, unless its capacity factor is 0
         if (capacityFactor > 0) {
            vn = (int) Math.round(capacityFactor / totalCapacity * totalVirtualNodes + 1);
         }
         virtualNodeCounts.put(member, vn);
      }

      HashMap<Integer, Address> primarySegments = new HashMap<Integer, Address>();
      for (int virtualNode = 0; virtualNode < totalVirtualNodes; virtualNode++) {
         for (Address member : sortedMembers) {
            if (virtualNode >= virtualNodeCounts.get(member))
               continue;

            // Add the virtual node count after applying MurmurHash on the node's hashCode
            // to make up for badly spread test addresses.
            int virtualNodeHash = normalizedHash(builder.getHashFunction(), member.hashCode());
            if (virtualNode != 0) {
               virtualNodeHash = normalizedHash(builder.getHashFunction(), virtualNodeHash + virtualNode);
            }
            int initSegment = virtualNodeHash / builder.getSegmentSize();
            for (int i = 0; i < numSegments; i++) {
               int segment = (initSegment + i) % numSegments;
               if (!primarySegments.containsKey(segment)) {
                  primarySegments.put(segment, member);
                  if (segment != initSegment) collisions++;
                  break;
               }
            }
         }
         if (primarySegments.size() >= numSegments)
            break;
      }

      return new TreeMap<Integer, Address>(primarySegments);
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
      HashSet<Address> leavers = new HashSet<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // Create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = null;

      // Remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[i] = owners;
         } else {
            // this segment has 0 owners, fix it
            if (rebalancedCH == null) {
               rebalancedCH = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers, actualCapacityFactors);
            }
            newSegmentOwners[i] = rebalancedCH.locateOwnersForSegment(i);
         }
      }

      return new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, numSegments, newMembers,
            actualCapacityFactors, newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), baseCH.getNumOwners(),
            baseCH.getNumSegments(), baseCH.getMembers(), baseCH.getCapacityFactors());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   protected static class Builder {
      private final Hash hashFunction;
      private final int numOwners;
      private final Map<Address, Float> capacityFactors;
      private final int actualNumOwners;
      private final int numSegments;
      private final List<Address> sortedMembers;
      private final int segmentSize;
      private final List<Address>[] segmentOwners;

      private Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                      Map<Address, Float> capacityFactors) {
         this.hashFunction = hashFunction;
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.capacityFactors = capacityFactors;
         this.actualNumOwners = Math.min(numOwners, members.size());
         this.sortedMembers = sort(members, capacityFactors);
         this.segmentSize = (int)Math.ceil((double)Integer.MAX_VALUE / numSegments);
         this.segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         }
      }

      public Hash getHashFunction() {
         return hashFunction;
      }

      public int getNumOwners() {
         return numOwners;
      }

      public int getActualNumOwners() {
         return actualNumOwners;
      }

      public int getNumSegments() {
         return numSegments;
      }

      public List<Address> getSortedMembers() {
         return sortedMembers;
      }

      public int getSegmentSize() {
         return segmentSize;
      }

      public List<Address>[] getAllOwners() {
         return segmentOwners;
      }

      public List<Address> getOwners(int i) {
         return segmentOwners[i];
      }

      public float getCapacityFactor(Address node) {
         return capacityFactors != null ? capacityFactors.get(node) : 1;
      }

      private List<Address> sort(List<Address> members, final Map<Address, Float> capacityFactors) {
         ArrayList<Address> result = new ArrayList<Address>(members);
         Collections.sort(result, new Comparator<Address>() {
            @Override
            public int compare(Address o1, Address o2) {
               // Sort descending by capacity factor and ascending by address (UUID)
               int capacityComparison = capacityFactors != null ? capacityFactors.get(o1).compareTo(capacityFactors.get(o2)) : 0;
               return capacityComparison != 0 ? -capacityComparison : o1.compareTo(o2);
            }
         });
         return result;
      }

      private int nodesWithLoad() {
         int nodesWithLoad = sortedMembers.size();
         if (capacityFactors != null) {
            nodesWithLoad = 0;
            for (Address node : sortedMembers) {
               if (capacityFactors.get(node) != 0) {
                  nodesWithLoad++;
               }
            }
         }
         return nodesWithLoad;
      }
   }

   public static class Externalizer extends AbstractExternalizer<SyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, SyncConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public SyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new SyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends SyncConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends SyncConsistentHashFactory>>singleton(SyncConsistentHashFactory.class);
      }
   }
}
