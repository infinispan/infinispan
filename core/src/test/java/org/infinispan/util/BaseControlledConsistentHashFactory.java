package org.infinispan.util;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Base consistent hash factory that contains a single segments
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@SuppressWarnings("unchecked")
public abstract class BaseControlledConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash>,
                                                                    Serializable {
   protected final int numSegments;

   protected BaseControlledConsistentHashFactory(int numSegments) {
      this.numSegments = numSegments;
   }

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(numSegments);
      List<Address>[] segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         segmentOwners[i] = createOwnersCollection(members, numOwners, i);
      }
      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, null, segmentOwners);
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
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

      DefaultConsistentHash updated = new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, numSegments, newMembers, null,
                                                                segmentOwners);
      return baseCH.equals(updated) ? baseCH : updated;
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalanced = create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(),
            baseCH.getMembers(), baseCH.getCapacityFactors());
      return baseCH.equals(rebalanced) ? baseCH : rebalanced;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      assertNumberOfSegments(ch1.getNumSegments());
      assertNumberOfSegments(ch2.getNumSegments());
      return ch1.union(ch2);
   }

   protected abstract List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex);

   private void assertNumberOfSegments(int numSegments) {
      assertEquals("Wrong number of segments.", this.numSegments, numSegments);
   }
}
