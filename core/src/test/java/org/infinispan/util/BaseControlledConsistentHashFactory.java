package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHash;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.remoting.transport.Address;

/**
 * Base consistent hash factory that contains a single segments
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@SuppressWarnings("unchecked")
public abstract class BaseControlledConsistentHashFactory<CH extends ConsistentHash> implements ConsistentHashFactory<CH>,
                                                                    Serializable, ExternalPojo {
   protected final Trait<CH> trait;
   protected final int numSegments;

   protected BaseControlledConsistentHashFactory(Trait<CH> trait, int numSegments) {
      this.trait = trait;
      this.numSegments = numSegments;
   }

   @Override
   public CH create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(numSegments);
      List<Address>[] segmentOwners = assignSegments(numOwners, numSegments, members);
      return create(hashFunction, numOwners, numSegments, members, capacityFactors, segmentOwners, false);
   }

   public List<Address>[] assignSegments(int numOwners, int numSegments, List<Address> members) {
      List<Address>[] segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         segmentOwners[i] = createOwnersCollection(members, numOwners, i);
      }
      return segmentOwners;
   }

   protected CH create(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors, List<Address>[] segmentOwners, boolean rebalanced) {
      return trait.create(hashFunction, numOwners, numSegments, members, capacityFactors, segmentOwners, rebalanced);
   }

   @Override
   public CH updateMembers(CH baseCH, List<Address> newMembers,
                                              Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(baseCH.getNumSegments());
      final int numOwners = baseCH.getNumOwners();
      List<Address>[] segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.retainAll(newMembers);
         if (owners.isEmpty()) {
            // updateMembers should only add new owners if there are no owners left
            owners = createOwnersCollection(newMembers, numOwners, i);
         }
         segmentOwners[i] = owners;
      }

      CH updated = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers, capacityFactors, segmentOwners, false);
      return baseCH.equals(updated) ? baseCH : updated;
   }

   @Override
   public CH rebalance(CH baseCH) {
      CH rebalanced = create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(),
            baseCH.getMembers(), baseCH.getCapacityFactors(), assignSegments(baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers()), true);
      return baseCH.equals(rebalanced) ? baseCH : rebalanced;
   }

   @Override
   public CH union(CH ch1, CH ch2) {
      assertNumberOfSegments(ch1.getNumSegments());
      assertNumberOfSegments(ch2.getNumSegments());
      return trait.union(ch1, ch2);
   }

   protected abstract List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex);

   private void assertNumberOfSegments(int numSegments) {
      assertEquals("Wrong number of segments.", this.numSegments, numSegments);
   }

   protected interface Trait<CH extends ConsistentHash> extends Serializable {
      CH create(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors, List<Address>[] segmentOwners, boolean rebalanced);
      CH union(CH ch1, CH ch2);
   }

   public static class DefaultTrait implements Trait<DefaultConsistentHash> {
      @Override
      public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors, List<Address>[] segmentOwners, boolean rebalanced) {
         return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, capacityFactors, segmentOwners);
      }

      @Override
      public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
         return ch1.union(ch2);
      }
   }

   public static class ScatteredTrait implements Trait<ScatteredConsistentHash> {
      @Override
      public ScatteredConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors, List<Address>[] segmentOwners, boolean rebalanced) {
         return new ScatteredConsistentHash(hashFunction, numSegments, members, capacityFactors, Stream.of(segmentOwners).map(list -> list.get(0)).toArray(Address[]::new), rebalanced);
      }

      @Override
      public ScatteredConsistentHash union(ScatteredConsistentHash ch1, ScatteredConsistentHash ch2) {
         return ch1.union(ch2);
      }
   }

   public static abstract class Default extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
      protected Default(int numSegments) {
         super(new DefaultTrait(), numSegments);
      }
   }

   public static abstract class Scattered extends BaseControlledConsistentHashFactory<ScatteredConsistentHash> {
      protected Scattered(int numSegments) {
         super(new ScatteredTrait(), numSegments);
      }
   }
}
