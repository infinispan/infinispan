package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Based on {@link DefaultConsistentHashFactory}.
 *
 * @since 9.0
 */
public class ScatteredConsistentHashFactory extends AbstractConsistentHashFactory<ScatteredConsistentHash> {

   private static final Log log = LogFactory.getLog(ScatteredConsistentHashFactory.class);

   @Override
   public ScatteredConsistentHash create(Hash hashFunction, int numOwners, int numSegments,
                                       List<Address> members, Map<Address, Float> capacityFactors) {
      if (numOwners != 1)
         throw new IllegalArgumentException("The number of owners is supposed to be 1");
      if (members.size() == 0)
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      checkCapacityFactors(members, capacityFactors);

      // Use the CH rebalance algorithm to get an even spread
      // Round robin doesn't work properly because a segment's owner must be unique,
      Builder builder = new Builder(hashFunction, numSegments, members, capacityFactors);
      rebalanceBuilder(builder);

      return builder.build();
   }

   @Override
   public ScatteredConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!ScatteredConsistentHash.class.getName().equals(consistentHashClass))
         throw log.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new ScatteredConsistentHash(state);
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
   public ScatteredConsistentHash updateMembers(ScatteredConsistentHash baseCH, List<Address> actualMembers,
                                              Map<Address, Float> actualCapacityFactors) {
      if (actualMembers.size() == 0)
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      checkCapacityFactors(actualMembers, actualCapacityFactors);

      boolean sameCapacityFactors = actualCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            actualCapacityFactors.equals(baseCH.getCapacityFactors());
      if (actualMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      // The builder constructor automatically removes leavers
      Builder builder = new Builder(baseCH, actualMembers, actualCapacityFactors);

      return builder.build();
   }

   @Override
   public ScatteredConsistentHash rebalance(ScatteredConsistentHash baseCH) {
      // This method assign new owners to the segments so that
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      Builder builder = new Builder(baseCH);
      rebalanceBuilder(builder);

      ScatteredConsistentHash balancedCH = builder.build();

      // we should return the base CH if we didn't change anything
      return balancedCH.equals(baseCH) ? baseCH : balancedCH;
   }

   /**
    * Merges two consistent hash objects that have the same number of segments, numOwners and hash function.
    * For each segment, the primary owner of the first CH has priority, the other primary owners become backups.
    */
   @Override
   public ScatteredConsistentHash union(ScatteredConsistentHash dch1, ScatteredConsistentHash dch2) {
      return dch1.union(dch2);
   }

   protected void rebalanceBuilder(Builder builder) {
      addFirstOwner(builder);

      replacePrimaryOwners(builder);

      builder.setRebalanced(true);
   }

   private void addFirstOwner(Builder builder) {
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         if (builder.getPrimaryOwner(segment) != null)
            continue;

         Address newPrimary = findNewPrimaryOwner(builder, builder.getMembers(), null);
         if (newPrimary != null) {
            builder.addPrimaryOwner(segment, newPrimary);
         }
      }
   }

   protected void replacePrimaryOwners(Builder builder) {
      // Find the node with the worst primary-owned-segments-to-capacity ratio W.
      // Iterate over all the segments primary-owned by W, and if possible replace it with another node.
      // After replacing, check that W is still the worst node. If not, repeat with the new worst node.
      // Keep track of the segments where we already replaced the primary owner, so we don't do it twice. ???
      boolean primaryOwnerReplaced = true;
      while (primaryOwnerReplaced) {
         Address worstNode = findWorstPrimaryOwner(builder, builder.getMembers());
         primaryOwnerReplaced = false;

         for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
            // Only replace if the worst node is the primary owner
            if (!Objects.equals(builder.getPrimaryOwner(segment), worstNode))
               continue;

            Address newPrimary = findNewPrimaryOwner(builder, builder.getMembers(), worstNode);
            if (newPrimary != null) {
               builder.addPrimaryOwner(segment, newPrimary);
               primaryOwnerReplaced = true;

               worstNode = findWorstPrimaryOwner(builder, builder.getMembers());
            }
         }
      }
   }

   protected static class Builder extends AbstractConsistentHashFactory.Builder {
      private final Address[] segmentOwners;
      private boolean isRebalanced;

      public Builder(Hash hashFunction, int numSegments, List<Address> members,
                     Map<Address, Float> capacityFactors) {
         super(hashFunction, new OwnershipStatistics(members), members, capacityFactors);
         this.segmentOwners = new Address[numSegments];
      }

      public Builder(ScatteredConsistentHash baseCH, List<Address> actualMembers,
                     Map<Address, Float> actualCapacityFactors) {
         super(baseCH.getHashFunction(), new OwnershipStatistics(baseCH, actualMembers), actualMembers, actualCapacityFactors);
         int numSegments = baseCH.getNumSegments();
         Set<Address> actualMembersSet = new HashSet<>(actualMembers);
         Address[] owners = new Address[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            Address owner = baseCH.locatePrimaryOwnerForSegment(segment);
            if (owner != null && actualMembersSet.contains(owner)) {
               owners[segment] = owner;
            }
         }
         this.segmentOwners = owners;
      }

      public Builder(ScatteredConsistentHash baseCH) {
         this(baseCH, baseCH.getMembers(), baseCH.getCapacityFactors());
      }

      public Builder(Builder other) {
         super(other);
         this.segmentOwners = Arrays.copyOf(other.segmentOwners, other.getNumSegments());
      }

      public int getNumSegments() {
         return segmentOwners.length;
      }

      public Address getPrimaryOwner(int segment) {
         return segmentOwners[segment];
      }

      public void addPrimaryOwner(int segment, Address newPrimaryOwner) {
         modCount++;
         if (segmentOwners[segment] != null) {
            stats.decPrimaryOwned(segmentOwners[segment]);
         }

         segmentOwners[segment] = newPrimaryOwner;
         stats.incOwned(newPrimaryOwner);
         stats.incPrimaryOwned(newPrimaryOwner);
      }

      public ScatteredConsistentHash build() {
         return new ScatteredConsistentHash(hashFunction, segmentOwners.length, members, capacityFactors, segmentOwners, isRebalanced);
      }

      public void setRebalanced(boolean isRebalanced) {
         this.isRebalanced = isRebalanced;
      }

      public int getPrimaryOwned(Address node) {
         return stats.getPrimaryOwned(node);
      }

      public int getOwned(Address node) {
         return stats.getOwned(node);
      }
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 4728;
   }

   public static class Externalizer extends AbstractExternalizer<ScatteredConsistentHashFactory> {

      @Override
      public void writeObject(UserObjectOutput output, ScatteredConsistentHashFactory chf) throws IOException {
      }

      @Override
      @SuppressWarnings("unchecked")
      public ScatteredConsistentHashFactory readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         return new ScatteredConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SCATTERED_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends ScatteredConsistentHashFactory>> getTypeClasses() {
         return Collections.singleton(ScatteredConsistentHashFactory.class);
      }
   }
}
