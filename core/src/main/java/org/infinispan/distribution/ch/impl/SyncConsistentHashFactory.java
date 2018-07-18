package org.infinispan.distribution.ch.impl;

import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * One of the assumptions people made on consistent hashing involves thinking
 * that given a particular key and same topology, it would produce the same
 * consistent hash value no matter which cache it was stored in. However,
 * that's not exactly the case in Infinispan.
 *
 * In order to the optimise the number of segments moved on join/leave,
 * Infinispan uses a consistent hash that depends on the previous consistent
 * hash. Given two caches, even if they contain exactly the same keyset, it's
 * very easy for the consistent hash history to differ, e.g. if 2 nodes join
 * you might see two separate topology change in one cache and a single
 * topology change in the other. The reason for that each node has to send a
 * {@link org.infinispan.topology.CacheTopologyControlCommand} for each cache
 * it wants to join and Infinispan can and does batch cache topology changes.
 * For example, if a rebalance is in progress, joins are queued and send in
 * one go when the rebalance has finished.
 *
 * This {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation avoids any of the issues
 * mentioned and guarantees that multiple caches with the same members will
 * have the same consistent hash.
 *
 * It has a drawback compared to {@link org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory} though:
 * it can potentially move a lot more segments during a rebalance than
 * strictly necessary because it's not taking advantage of the optimisation
 * mentioned above.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   private static final Log log = LogFactory.getLog(SyncConsistentHashFactory.class);
   public static final float OWNED_SEGMENTS_ALLOWED_VARIATION = 1.10f;
   public static final float PRIMARY_SEGMENTS_ALLOWED_VARIATION = 1.05f;

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      checkCapacityFactors(members, capacityFactors);

      Builder builder = createBuilder(hashFunction, numOwners, numSegments, members, capacityFactors);
      builder.populateOwners(numSegments);
      builder.copyOwners();

      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, capacityFactors, builder.segmentOwners);
   }

   @Override
   public DefaultConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!DefaultConsistentHash.class.getName().equals(consistentHashClass))
         throw log.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new DefaultConsistentHash(state);
   }

   protected Builder createBuilder(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      return new Builder(hashFunction, numOwners, numSegments, members, capacityFactors);
   }

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
      protected final Hash hashFunction;
      protected final int numOwners;
      protected final Map<Address, Float> capacityFactors;
      protected final int actualNumOwners;
      protected final int numSegments;
      protected final List<Address> sortedMembers;
      protected final int segmentSize;
      protected final List<Address>[] segmentOwners;
      protected final OwnershipStatistics stats;

      protected boolean ignoreMaxSegments;

      protected Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                      Map<Address, Float> capacityFactors) {
         this.hashFunction = hashFunction;
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.actualNumOwners = Math.min(numOwners, members.size());
         this.sortedMembers = sort(members, capacityFactors);
         this.capacityFactors = populateCapacityFactors(capacityFactors, sortedMembers);
         this.segmentSize = Util.getSegmentSize(numSegments);
         this.segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         }
         stats = new OwnershipStatistics(members);
      }

      private Map<Address, Float> populateCapacityFactors(Map<Address, Float> capacityFactors, List<Address> sortedMembers) {
         if (capacityFactors != null)
            return capacityFactors;

         Map<Address, Float> realCapacityFactors = new HashMap<>();
         for (Address member : sortedMembers) {
            realCapacityFactors.put(member, 1.0f);
         }
         return realCapacityFactors;
      }

      protected void addOwnerNoCheck(int segment, Address owner) {
         segmentOwners[segment].add(owner);
         stats.incOwned(owner);
         if (segmentOwners[segment].size() == 1) {
            stats.incPrimaryOwned(owner);
         }
      }

      protected float computeTotalCapacity() {
         if (capacityFactors == null)
            return sortedMembers.size();

         float totalCapacity = 0;
         for (Address member : sortedMembers) {
            Float capacityFactor = capacityFactors.get(member);
            totalCapacity += capacityFactor;
         }
         return totalCapacity;
      }

      protected List<Address> sort(List<Address> members, final Map<Address, Float> capacityFactors) {
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

      protected void copyOwners() {
         ignoreMaxSegments = false;
         doCopyOwners();
         ignoreMaxSegments = true;
         doCopyOwners();
      }

      protected void doCopyOwners() {
         // The primary owners have been already assigned (and sometimes backup owners as well).
         // For each segment with not enough owners, add the owners from the previous segments.
         for (int segment = 0; segment < numSegments; segment++) {
            List<Address> owners = segmentOwners[segment];
            int additionalOwnersSegment = nextSegment(segment);
            while (canAddOwners(owners) && additionalOwnersSegment != segment) {
               List<Address> additionalOwners = segmentOwners[additionalOwnersSegment];
               for (Address additionalOwner : additionalOwners) {
                  addBackupOwner(segment, additionalOwner);
                  if (!canAddOwners(owners))
                     break;
               }
               additionalOwnersSegment = nextSegment(additionalOwnersSegment);
            }
         }
      }

      protected boolean canAddOwners(List<Address> owners) {
         return owners.size() < actualNumOwners;
      }

      protected int nextSegment(int segment) {
         if (segment == numSegments - 1)
            return 0;

         return segment + 1;

      }

      protected void populateOwners(int numSegments) {
         int virtualNode = 0;
         // Loop until we have already assigned a primary owner to each segment
         do {
            for (Address member : sortedMembers) {
               int segment = computeSegment(member, virtualNode);
               addPrimaryOwner(segment, member);
            }
            virtualNode++;
         } while (stats.sumPrimaryOwned() < numSegments);

         // If there are too few segments, some members may not have any segments at this point
         // Loop until we have assigned at least one segment to each member
         // or until we have assigned numOwners owners to all segments
         virtualNode = 0;
         boolean membersWithZeroSegments = false;
         do {
            for (Address member : sortedMembers) {
               if (stats.getOwned(member) > 0)
                  continue;

               membersWithZeroSegments = true;
               int segment = computeSegment(member, virtualNode);
               addBackupOwner(segment, member);
            }
            virtualNode++;
         } while (membersWithZeroSegments && stats.sumOwned() < numSegments);
      }

      private int computeSegment(Address member, int virtualNode) {
         // Add the virtual node count after applying MurmurHash on the node's hashCode
         // to make up for badly spread test addresses.
         int virtualNodeHash = normalizedHash(hashFunction, member.hashCode());
         if (virtualNode != 0) {
            virtualNodeHash = normalizedHash(hashFunction, virtualNodeHash + virtualNode);
         }
         return virtualNodeHash / segmentSize;
      }

      protected double computeExpectedSegmentsForNode(Address node, int numCopies) {
         Float nodeCapacityFactor = capacityFactors.get(node);
         if (nodeCapacityFactor == 0)
            return 0;

         double remainingCapacity = computeTotalCapacity();
         double remainingCopies = numCopies * numSegments;
         for (Address a : sortedMembers) {
            float capacityFactor = capacityFactors.get(a);
            double nodeSegments = capacityFactor / remainingCapacity * remainingCopies;
            if (nodeSegments > numSegments) {
               nodeSegments = numSegments;
               remainingCapacity -= capacityFactor;
               remainingCopies -= nodeSegments;
               if (node.equals(a))
                  return nodeSegments;
            } else {
               // All the nodes from now on will have less than numSegments segments, so we can stop the iteration
               if (!node.equals(a)) {
                  nodeSegments = nodeCapacityFactor / remainingCapacity * remainingCopies;
               }
               return Math.max(nodeSegments, 1);
            }
         }
         throw new IllegalStateException("The nodes collection does not include " + node);
      }

      protected boolean addPrimaryOwner(int segment, Address candidate) {
         List<Address> owners = segmentOwners[segment];
         if (owners.isEmpty()) {
            double expectedSegments = computeExpectedSegmentsForNode(candidate, 1);
            long maxSegments = Math.round(Math.ceil(expectedSegments) * PRIMARY_SEGMENTS_ALLOWED_VARIATION);
            if (stats.getPrimaryOwned(candidate) < maxSegments) {
               addOwnerNoCheck(segment, candidate);
               return true;
            }
         }
         return false;
      }

      protected boolean addBackupOwner(int segment, Address candidate) {
         List<Address> owners = segmentOwners[segment];
         if (owners.size() < actualNumOwners && !owners.contains(candidate)) {
            if (!ignoreMaxSegments) {
               double expectedSegments = computeExpectedSegmentsForNode(candidate, actualNumOwners);
               long maxSegments = Math.round(Math.ceil(expectedSegments) * OWNED_SEGMENTS_ALLOWED_VARIATION);

               if (stats.getOwned(candidate) < maxSegments) {
                  addOwnerNoCheck(segment, candidate);
                  return true;
               }
            } else {
               if (!capacityFactors.get(candidate).equals(0f)) {
                  addOwnerNoCheck(segment, candidate);
                  return true;
               }
            }
         }
         return false;
      }

      protected int normalizedHash(Hash hashFunction, int hashcode) {
         return hashFunction.hash(hashcode) & Integer.MAX_VALUE;
      }
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return -10007;
   }

   public static class Externalizer extends AbstractExternalizer<SyncConsistentHashFactory> {

      @Override
      public void writeObject(UserObjectOutput output, SyncConsistentHashFactory chf) {
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
