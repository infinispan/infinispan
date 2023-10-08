package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.transport.Address;

/**
 * Base consistent hash factory that contains a single segments
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@SuppressWarnings("unchecked")
public abstract class BaseControlledConsistentHashFactory<CH extends ConsistentHash> implements ConsistentHashFactory<CH> {
   protected Trait<CH> trait;

   @ProtoField(number = 1, defaultValue = "0")
   public int numSegments;

   protected BaseControlledConsistentHashFactory() {}

   protected BaseControlledConsistentHashFactory(Trait<CH> trait, int numSegments) {
      this.trait = trait;
      this.numSegments = numSegments;
   }

   @Override
   public CH create(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(numSegments);
      List<Address>[] segmentOwners = assignSegments(numSegments, numOwners, members);
      return create(numOwners, numSegments, members, capacityFactors, segmentOwners, false);
   }

   private List<Address>[] assignSegments(int numSegments, int numOwners, List<Address> members) {
      int[][] ownerIndexes = assignOwners(numSegments, members);
      return Arrays.stream(ownerIndexes)
                   .map(indexes -> Arrays.stream(indexes)
                                         .mapToObj(members::get)
                                         .collect(Collectors.toList()))
                   .map(indexes -> indexes.subList(0, Math.min(indexes.size(), numOwners)))
                   .toArray((IntFunction<List<Address>[]>) List[]::new);
   }

   protected CH create(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors,
                       List<Address>[] segmentOwners, boolean rebalanced) {
      return trait.create(numOwners, numSegments, members, capacityFactors, segmentOwners, rebalanced);
   }

   @Override
   public CH updateMembers(CH baseCH, List<Address> newMembers, Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(baseCH.getNumSegments());
      List<Address>[] segmentOwners = new List[numSegments];
      List<Address>[] balancedOwners = null;
      int numOwners = trait.getNumOwners(baseCH);
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<>(baseCH.locateOwnersForSegment(i));
         owners.retainAll(newMembers);
         // updateMembers should only add new owners if there are no owners left and the trait requires a primary owner
         if (owners.isEmpty() && trait.requiresPrimaryOwner()) {
            if (balancedOwners == null) {
               balancedOwners = assignSegments(numSegments, numOwners, newMembers);
            }
            owners = balancedOwners[i];
         }
         segmentOwners[i] = owners;
      }

      CH updated = create(numOwners, numSegments, newMembers, capacityFactors, segmentOwners, false);
      return baseCH.equals(updated) ? baseCH : updated;
   }

   @Override
   public CH rebalance(CH baseCH) {
      int numOwners = trait.getNumOwners(baseCH);
      List<Address>[] owners = assignSegments(baseCH.getNumSegments(), numOwners, baseCH.getMembers());
      CH rebalanced = create(numOwners, baseCH.getNumSegments(), baseCH.getMembers(), baseCH.getCapacityFactors(), owners, true);
      return baseCH.equals(rebalanced) ? baseCH : rebalanced;
   }

   @Override
   public CH union(CH ch1, CH ch2) {
      assertNumberOfSegments(ch1.getNumSegments());
      assertNumberOfSegments(ch2.getNumSegments());
      return trait.union(ch1, ch2);
   }

   protected abstract int[][] assignOwners(int numSegments, List<Address> members);

   private void assertNumberOfSegments(int numSegments) {
      assertEquals("Wrong number of segments.", this.numSegments, numSegments);
   }

   protected interface Trait<CH extends ConsistentHash> {
      CH create(int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors,
                List<Address>[] segmentOwners, boolean rebalanced);
      CH union(CH ch1, CH ch2);

      boolean requiresPrimaryOwner();

      int getNumOwners(CH ch);
   }

   public static class DefaultTrait implements Trait<DefaultConsistentHash> {
      @Override
      public DefaultConsistentHash create(int numOwners, int numSegments, List<Address> members,
                                          Map<Address, Float> capacityFactors, List<Address>[] segmentOwners,
                                          boolean rebalanced) {
         return new DefaultConsistentHash(numOwners, numSegments, members, capacityFactors, segmentOwners);
      }

      @Override
      public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
         return ch1.union(ch2);
      }

      @Override
      public boolean requiresPrimaryOwner() {
         return true;
      }

      @Override
      public int getNumOwners(DefaultConsistentHash defaultConsistentHash) {
         return defaultConsistentHash.getNumOwners();
      }
   }

   public static class ReplicatedTrait implements Trait<ReplicatedConsistentHash> {
      @Override
      public ReplicatedConsistentHash create(int numOwners, int numSegments, List<Address> members,
                                            Map<Address, Float> capacityFactors, List<Address>[] segmentOwners,
                                            boolean rebalanced) {
         List<Integer> segmentOwners1 = Stream.of(segmentOwners)
                                      .map(list -> members.indexOf(list.get(0)))
                                      .toList();
         // No support for zero-capacity nodes for now
         return new ReplicatedConsistentHash(members, capacityFactors, Collections.emptyList(), segmentOwners1);
      }

      @Override
      public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
         return ch1.union(ch2);
      }

      @Override
      public boolean requiresPrimaryOwner() {
         return true;
      }

      @Override
      public int getNumOwners(ReplicatedConsistentHash ReplicatedConsistentHash) {
         return 1;
      }
   }

   public static abstract class Default extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
      protected Default(int numSegments) {
         super(new DefaultTrait(), numSegments);
      }
   }
}
