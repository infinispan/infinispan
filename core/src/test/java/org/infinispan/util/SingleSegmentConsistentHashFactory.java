package org.infinispan.util;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.io.Serializable;
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
public abstract class SingleSegmentConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash>,
                                                                    Serializable {

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(numSegments);
      return new DefaultConsistentHash(hashFunction, numOwners, 1, members, null, new List[]{createOwnersCollection(members, numOwners)});
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
                                              Map<Address, Float> capacityFactors) {
      assertNumberOfSegments(baseCH.getNumSegments());
      final int numOwners = baseCH.getNumOwners();
      DefaultConsistentHash updated = new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, 1, newMembers, null,
                                                                new List[]{createOwnersCollection(baseCH.getMembers(), numOwners)});
      return baseCH.equals(updated) ? baseCH : updated;
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      assertNumberOfSegments(baseCH.getNumSegments());
      final List<Address> members = baseCH.getMembers();
      final int numOwners = baseCH.getNumOwners();
      DefaultConsistentHash rebalanced = new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, 1, members, null,
                                                                   new List[]{createOwnersCollection(members, numOwners)});
      return baseCH.equals(rebalanced) ? baseCH : rebalanced;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      assertNumberOfSegments(ch1.getNumSegments());
      assertNumberOfSegments(ch2.getNumSegments());
      return ch1.union(ch2);
   }

   protected abstract List<Address> createOwnersCollection(List<Address> members, int numberOfOwners);

   private void assertNumberOfSegments(int numSegments) {
      assertEquals("Wrong number of segments.", 1, numSegments);
   }
}
