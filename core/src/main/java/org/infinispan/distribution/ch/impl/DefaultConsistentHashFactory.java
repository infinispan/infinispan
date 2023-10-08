package org.infinispan.distribution.ch.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Default implementation of {@link ConsistentHashFactory}.
 *
 * All methods except {@link #union(DefaultConsistentHash, DefaultConsistentHash)} return a consistent hash
 * with floor(numOwners*numSegments/numNodes) &lt;= segments per owner &lt;= ceil(numOwners*numSegments/numNodes).
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.DEFAULT_CONSISTENT_HASH_FACTORY)
public class DefaultConsistentHashFactory extends AbstractConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(int numOwners, int numSegments,
                                       List<Address> members, Map<Address, Float> capacityFactors) {
      if (members.isEmpty())
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      if (numOwners <= 0)
         throw new IllegalArgumentException("The number of owners should be greater than 0");
      checkCapacityFactors(members, capacityFactors);

      // Use the CH rebalance algorithm to get an even spread
      // Round robin doesn't work properly because a segment's owner must be unique,
      Builder builder = new Builder(numOwners, numSegments, members, capacityFactors);
      rebalanceBuilder(builder);

      return builder.build();
   }

   @Override
   public DefaultConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!DefaultConsistentHash.class.getName().equals(consistentHashClass))
         throw CONTAINER.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new DefaultConsistentHash(state);
   }

   /**
    * Leavers are removed and segments without owners are assigned new owners. Joiners might get some of the un-owned
    * segments but otherwise they are not taken into account (that should happen during a rebalance).
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @param actualMembers A list of addresses representing the new cache members.
    * @return
    */
   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> actualMembers,
                                              Map<Address, Float> actualCapacityFactors) {
      if (actualMembers.isEmpty())
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      checkCapacityFactors(actualMembers, actualCapacityFactors);

      boolean sameCapacityFactors = actualCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            actualCapacityFactors.equals(baseCH.getCapacityFactors());
      if (actualMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      // The builder constructor automatically removes leavers
      Builder builder = new Builder(baseCH, actualMembers, actualCapacityFactors);

      // If there are segments with 0 owners, fix them
      // Try to assign the same owners for those segments as a future rebalance call would.
      Builder balancedBuilder = null;
      for (int segment = 0; segment < baseCH.getNumSegments(); segment++) {
         if (builder.getOwners(segment).isEmpty()) {
            if (balancedBuilder == null) {
               balancedBuilder = new Builder(builder);
               rebalanceBuilder(balancedBuilder);
            }
            builder.addOwners(segment, balancedBuilder.getOwners(segment));
         }
      }
      return builder.build();
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      // This method assign new owners to the segments so that
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      // * floor(numSegments*numOwners/numNodes) <= num_segments_owned(n) for each node n
      // * num_segments_owned(n) <= ceil(numSegments*numOwners/numNodes) for each node n
      Builder builder = new Builder(baseCH);
      rebalanceBuilder(builder);

      DefaultConsistentHash balancedCH = builder.build();

      // we should return the base CH if we didn't change anything
      return balancedCH.equals(baseCH) ? baseCH : balancedCH;
   }

   /**
    * Merges two consistent hash objects that have the same number of segments, numOwners and hash function.
    * For each segment, the primary owner of the first CH has priority, the other primary owners become backups.
    */
   @Override
   public DefaultConsistentHash union(DefaultConsistentHash dch1, DefaultConsistentHash dch2) {
      return dch1.union(dch2);
   }

   protected void rebalanceBuilder(Builder builder) {
      addPrimaryOwners(builder);
      addBackupOwners(builder);
   }

   protected void addPrimaryOwners(Builder builder) {
      addFirstOwner(builder);

      // 1. Try to replace primary owners with too many segments with the backups in those segments.
      swapPrimaryOwnersWithBackups(builder);

      // 2. For segments that don't have enough owners, try to add a new owner as the primary owner.
      int actualNumOwners = builder.getActualNumOwners();
      replacePrimaryOwners(builder, actualNumOwners);

      // 3. If some primary owners still have too many segments, allow adding an extra owner as the primary owner.
      // Since a segment only has 1 primary owner, this will be enough to give us a "proper" primary owner
      // for each segment.
      replacePrimaryOwners(builder, actualNumOwners + 1);
   }

   private void addFirstOwner(Builder builder) {
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         if (!builder.getOwners(segment).isEmpty())
            continue;

         Address newPrimary = findNewPrimaryOwner(builder, builder.getMembers(), null);
         if (newPrimary != null) {
            builder.addPrimaryOwner(segment, newPrimary);
         }
      }
   }

   protected void replacePrimaryOwners(Builder builder, int maxOwners) {
      // Find the node with the worst primary-owned-segments-to-capacity ratio W.
      // Iterate over all the segments primary-owned by W, and if possible replace it with another node.
      // After replacing, check that W is still the worst node. If not, repeat with the new worst node.
      // Keep track of the segments where we already replaced the primary owner, so we don't do it twice. ???
      boolean primaryOwnerReplaced = true;
      while (primaryOwnerReplaced) {
         Address worstNode = findWorstPrimaryOwner(builder, builder.getMembers());
         primaryOwnerReplaced = false;

         for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
            if (builder.getOwners(segment).size() >= maxOwners)
               continue;

            // Only replace if the worst node is the primary owner
            if (!builder.getPrimaryOwner(segment).equals(worstNode))
               continue;

            Address newPrimary = findNewPrimaryOwner(builder, builder.getMembers(), worstNode);
            if (newPrimary != null && !builder.getOwners(segment).contains(newPrimary)) {
               builder.addPrimaryOwner(segment, newPrimary);
               primaryOwnerReplaced = true;

               worstNode = findWorstPrimaryOwner(builder, builder.getMembers());
            }
         }
      }
   }

   protected void swapPrimaryOwnersWithBackups(Builder builder) {
      // If a segment has primaryOwned(primaryOwner(segment)) > maxPrimarySegments,
      // try to swap the primary owner with one of the backup owners.
      // The new primary owner must primary-own < minPrimarySegments segments.
      // Iterate in reverse order so the CH looks more stable in the logs as we add nodes
      for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
         if (builder.getOwners(segment).isEmpty())
            continue;

         Address primaryOwner = builder.getPrimaryOwner(segment);
         Address newPrimary = findNewPrimaryOwner(builder, builder.getBackupOwners(segment), primaryOwner);
         if (newPrimary != null) {
            // actually replaces the primary owner
            builder.replacePrimaryOwnerWithBackup(segment, newPrimary);
         }
      }
   }

   protected void addBackupOwners(Builder builder) {
      // 1. Remove extra owners (could be leftovers from addPrimaryOwners).
      removeExtraBackupOwners(builder);

      // 2. If owners(segment) < numOwners, add new owners.
      // We always add the member that is not an owner already and has the best owned-segments-to-capacity ratio.
      doAddBackupOwners(builder);

      // 3. Now owners(segment) == numOwners for every segment because of steps 1 and 2.
      // For each owner, if there exists a non-owner with a better owned-segments-to-capacity-ratio, replace it.
      replaceBackupOwners(builder);
   }

   protected void removeExtraBackupOwners(Builder builder) {
      // Find the node with the worst segments-to-capacity ratio, and replace it in one of the owner lists
      // Repeat with the next-worst node, and so on.
      List<Address> untestedNodes = new ArrayList<Address>(builder.getMembers());
      while (!untestedNodes.isEmpty()) {
         boolean ownerRemoved = false;
         Address worstNode = findWorstBackupOwner(builder, untestedNodes);

         // Iterate in reverse order so the CH looks more stable in the logs as we add nodes
         for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
            List<Address> owners = builder.getOwners(segment);
            if (owners.size() <= builder.getActualNumOwners())
               continue;

            int ownerIdx = owners.indexOf(worstNode);
            // Don't remove the primary
            if (ownerIdx > 0) {
               builder.removeOwner(segment, worstNode);
               ownerRemoved = true;
               // The worst node might have changed.
               untestedNodes = new ArrayList<Address>(builder.getMembers());
               worstNode = findWorstBackupOwner(builder, untestedNodes);
            }
         }
         if (!ownerRemoved) {
            untestedNodes.remove(worstNode);
         }
      }
   }

   /**
    * @return The worst backup owner, or {@code null} if the remaining nodes own 0 segments.
    */
   private Address findWorstBackupOwner(Builder builder, List<Address> nodes) {
      Address worst = null;
      float maxSegmentsPerCapacity = -1;
      for (Address owner : nodes) {
         float capacityFactor = builder.getCapacityFactor(owner);
         if (worst == null || builder.getOwned(owner) - 1 >= capacityFactor * maxSegmentsPerCapacity) {
            worst = owner;
            maxSegmentsPerCapacity = capacityFactor != 0 ? (builder.getOwned(owner) - 1) / capacityFactor : 0;
         }
      }
      return worst;
   }

   protected void doAddBackupOwners(Builder builder) {
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);

         while (owners.size() < builder.getActualNumOwners()) {
            Address newOwner = findNewBackupOwner(builder, owners, null);
            builder.addOwner(segment, newOwner);
         }
      }
   }

   protected void replaceBackupOwners(Builder builder) {
      // Find the node with the worst segments-to-capacity ratio, and replace it in one of the owner lists.
      // If it's not possible to replace any owner with the worst node, remove the worst from the untested nodes
      // list and try with the new worst, repeating as necessary. After replacing one owner,
      // go back to the original untested nodes list.
      List<Address> untestedNodes = new ArrayList<Address>(builder.getMembers());
      while (!untestedNodes.isEmpty()) {
         Address worstNode = findWorstBackupOwner(builder, untestedNodes);
         boolean backupOwnerReplaced = false;

         // Iterate in reverse order so the CH looks more stable in the logs as we add nodes
         for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
            List<Address> owners = builder.getOwners(segment);
            int ownerIdx = owners.indexOf(worstNode);
            // Don't replace the primary
            if (ownerIdx <= 0)
               continue;

            // Surely there is a better node to replace this owner with...
            Address replacement = findNewBackupOwner(builder, owners, worstNode);
            if (replacement != null) {
               //log.tracef("Segment %3d: replacing owner %s with %s", segment, worstNode, replacement);
               builder.removeOwner(segment, worstNode);
               builder.addOwner(segment, replacement);
               backupOwnerReplaced = true;
               // The worst node might have changed.
               untestedNodes = new ArrayList<Address>(builder.getMembers());
               worstNode = findWorstBackupOwner(builder, untestedNodes);
            }
         }

         if (!backupOwnerReplaced) {
            untestedNodes.remove(worstNode);
         }
      }
   }

   /**
    * @return The member with the worst owned segments/capacity ratio that is also not in the excludes list.
    */
   protected Address findNewBackupOwner(Builder builder, Collection<Address> excludes, Address owner) {
      // We want the owned/capacity ratio of the actual owner after removing the current segment to be bigger
      // than the owned/capacity ratio of the new owner after adding the current segment, so that a future pass
      // won't try to switch them back.
      Address best = null;
      float initialCapacityFactor = owner != null ? builder.getCapacityFactor(owner) : 0;
      float bestSegmentsPerCapacity = initialCapacityFactor != 0 ? (builder.getOwned(owner) - 1 ) / initialCapacityFactor :
            Float.MAX_VALUE;
      for (Address candidate : builder.getMembers()) {
         if (excludes == null || !excludes.contains(candidate)) {
            int owned = builder.getOwned(candidate);
            float capacityFactor = builder.getCapacityFactor(candidate);
            if ((owned + 1) <= capacityFactor * bestSegmentsPerCapacity) {
               best = candidate;
               bestSegmentsPerCapacity = (owned + 1) / capacityFactor;
            }
         }
      }

      return best;
   }

   protected static class Builder extends AbstractConsistentHashFactory.Builder {
      private final int initialNumOwners;
      private final int actualNumOwners;
      private final List<Address>[] segmentOwners;

      public Builder(int numOwners, int numSegments, List<Address> members,
                     Map<Address, Float> capacityFactors) {
         super(new OwnershipStatistics(members), members, capacityFactors);
         this.initialNumOwners = numOwners;
         this.actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);
         this.segmentOwners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            segmentOwners[segment] = new ArrayList<Address>(actualNumOwners);
         }
      }

      public Builder(DefaultConsistentHash baseCH, List<Address> actualMembers,
                     Map<Address, Float> actualCapacityFactors) {
         super(new OwnershipStatistics(baseCH, actualMembers), actualMembers, actualCapacityFactors);
         int numSegments = baseCH.getNumSegments();
         Set<Address> actualMembersSet = new HashSet<Address>(actualMembers);
         List[] owners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            owners[segment] = new ArrayList<>(baseCH.locateOwnersForSegment(segment));
            owners[segment].retainAll(actualMembersSet);
         }
         this.initialNumOwners = baseCH.getNumOwners();
         this.actualNumOwners = computeActualNumOwners(initialNumOwners, actualMembers, actualCapacityFactors);
         this.segmentOwners = owners;
      }

      public Builder(DefaultConsistentHash baseCH) {
         this(baseCH, baseCH.getMembers(), baseCH.getCapacityFactors());
      }

      public Builder(Builder other) {
         super(other);
         int numSegments = other.getNumSegments();
         List[] owners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            owners[segment] = new ArrayList<Address>(other.segmentOwners[segment]);
         }
         this.initialNumOwners = other.initialNumOwners;
         this.actualNumOwners = other.actualNumOwners;
         this.segmentOwners = owners;
      }

      public int getActualNumOwners() {
         return actualNumOwners;
      }

      public int getNumSegments() {
         return segmentOwners.length;
      }

      public List<Address> getOwners(int segment) {
         return segmentOwners[segment];
      }

      public Address getPrimaryOwner(int segment) {
         return segmentOwners[segment].get(0);
      }

      public List<Address> getBackupOwners(int segment) {
         return segmentOwners[segment].subList(1, segmentOwners[segment].size());
      }

      public boolean addOwner(int segment, Address owner) {
         modCount++;
         List<Address> thisSegmentOwners = segmentOwners[segment];
         if (thisSegmentOwners.contains(owner))
            return false;

         thisSegmentOwners.add(owner);
         stats.incOwned(owner);
         if (thisSegmentOwners.size() == 1) {
            // the first owner
            stats.incPrimaryOwned(owner);
         }
         return true;
      }

      public boolean addOwners(int segment, Collection<Address> newOwners) {
         boolean modified = false;
         for (Address owner : newOwners) {
            modified |= addOwner(segment, owner);
         }
         return modified;
      }

      public boolean removeOwner(int segment, Address owner) {
         modCount++;
         List<Address> segmentOwners = this.segmentOwners[segment];
         if (segmentOwners.get(0).equals(owner)) {
            stats.decPrimaryOwned(owner);
         }
         boolean modified = segmentOwners.remove(owner);
         if (modified) {
            stats.decOwned(owner);
         }
         return modified;
      }

      public void addPrimaryOwner(int segment, Address newPrimaryOwner) {
         modCount++;
         List<Address> segmentOwners = this.segmentOwners[segment];
         int ownerIndex = segmentOwners.indexOf(newPrimaryOwner);
         if (ownerIndex >= 0) {
            throw new IllegalArgumentException("The new primary owner must not be a backup already");
         }

         if (!segmentOwners.isEmpty()) {
            Address oldPrimaryOwner = segmentOwners.get(0);
            stats.decPrimaryOwned(oldPrimaryOwner);
         }

         segmentOwners.add(0, newPrimaryOwner);
         stats.incOwned(newPrimaryOwner);
         stats.incPrimaryOwned(newPrimaryOwner);
      }

      public void replacePrimaryOwnerWithBackup(int segment, Address newPrimaryOwner) {
         modCount++;
         List<Address> segmentOwners = this.segmentOwners[segment];
         int ownerIndex = segmentOwners.indexOf(newPrimaryOwner);
         if (ownerIndex <= 0) {
            throw new IllegalArgumentException("The new primary owner must already be a backup owner");
         }

         Address oldPrimaryOwner = segmentOwners.get(0);
         stats.decPrimaryOwned(oldPrimaryOwner);

         segmentOwners.remove(ownerIndex);
         segmentOwners.add(0, newPrimaryOwner);
         stats.incPrimaryOwned(newPrimaryOwner);
      }

      public DefaultConsistentHash build() {
         return new DefaultConsistentHash(initialNumOwners, segmentOwners.length, members, capacityFactors,
               segmentOwners);
      }

      public int getPrimaryOwned(Address node) {
         return stats.getPrimaryOwned(node);
      }

      public int getOwned(Address node) {
         return stats.getOwned(node);
      }

      public int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
         int nodesWithLoad = members.size();
         if (capacityFactors != null) {
            nodesWithLoad = 0;
            for (Address node : members) {
               if (capacityFactors.get(node) != 0) {
                  nodesWithLoad++;
               }
            }
         }
         return Math.min(numOwners, nodesWithLoad);
      }
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 3853;
   }
}
