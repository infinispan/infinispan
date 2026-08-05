package org.infinispan.distribution.ch.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tests cross-cache determinism for {@link RendezvousConsistentHashFactory}.
 */
@Test(groups = "unit", testName = "distribution.ch.RendezvousConsistentHashDeterminismTest")
public class RendezvousConsistentHashDeterminismTest extends AbstractInfinispanTest {

   public void testSameMembersProduceSameCH() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      List<Address> members = Arrays.asList(A, B, C);

      ConsistentHashFactory<DefaultConsistentHash> chf = RendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash ch1 = chf.create(2, 64, members, null);
      DefaultConsistentHash ch2 = chf.create(2, 64, members, null);

      assertEquals(ch1, ch2, "Two create() calls with identical members must produce equal CHs");
   }

   public void testDifferentMemberOrderProducesSameCH() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");

      ConsistentHashFactory<DefaultConsistentHash> chf = RendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash ch1 = chf.create(2, 64, Arrays.asList(A, B, C), null);
      DefaultConsistentHash ch2 = chf.create(2, 64, Arrays.asList(C, A, B), null);

      // The segment assignments must be identical regardless of member list order.
      // Note: ch1.equals(ch2) would also check member list order which DefaultConsistentHash
      // preserves as-passed. Compare per-segment instead.
      for (int s = 0; s < ch1.getNumSegments(); s++) {
         assertEquals(new java.util.HashSet<>(ch1.locateOwnersForSegment(s)),
               new java.util.HashSet<>(ch2.locateOwnersForSegment(s)),
               "Segment " + s + " owner sets must match regardless of member list order");
      }
   }

   public void testSameMembersAcrossMultipleCachesAlignPrimaries() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      List<Address> members = Arrays.asList(A, B, C);

      ConsistentHashFactory<DefaultConsistentHash> chf = RendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash cache1CH = chf.create(2, 64, members, null);
      DefaultConsistentHash cache2CH = chf.create(2, 64, members, null);

      for (int s = 0; s < cache1CH.getNumSegments(); s++) {
         assertEquals(
               cache1CH.locatePrimaryOwnerForSegment(s),
               cache2CH.locatePrimaryOwnerForSegment(s),
               "Primary owner for segment " + s + " must be the same across caches with identical members"
         );
      }
   }

   public void testPrimaryAlignmentSurvivesCapacityFactorScaling() {
      // All factors 1.0 vs all factors 2.0 — only ratios matter, output must be identical
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      List<Address> members = Arrays.asList(A, B, C);

      Map<Address, Float> cf1 = new HashMap<>();
      cf1.put(A, 1f);
      cf1.put(B, 1f);
      cf1.put(C, 1f);

      Map<Address, Float> cf2 = new HashMap<>();
      cf2.put(A, 2f);
      cf2.put(B, 2f);
      cf2.put(C, 2f);

      ConsistentHashFactory<DefaultConsistentHash> chf = RendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash ch1 = chf.create(2, 64, members, cf1);
      DefaultConsistentHash ch2 = chf.create(2, 64, members, cf2);

      assertEquals(ch1, ch2, "Uniform scaling of capacity factors must produce identical CH");
   }

   public void testTopologyAwareVariantIsAlsoDeterministic() {
      Address A = Address.random("A", "s1", null, null);
      Address B = Address.random("B", "s2", null, null);
      Address C = Address.random("C", "s3", null, null);
      List<Address> members = Arrays.asList(A, B, C);

      ConsistentHashFactory<DefaultConsistentHash> chf = TopologyAwareRendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash ch1 = chf.create(2, 64, members, null);
      DefaultConsistentHash ch2 = chf.create(2, 64, members, null);

      assertEquals(ch1, ch2, "TopologyAwareRendezvous must also be deterministic");
   }

   public void testMemberListWithDifferentUUIDsProducesDifferentCH() {
      // Same count, different UUIDs — should produce different CHs
      Address A1 = Address.random("A1");
      Address B1 = Address.random("B1");
      Address C1 = Address.random("C1");

      Address A2 = Address.random("A2");
      Address B2 = Address.random("B2");
      Address C2 = Address.random("C2");

      ConsistentHashFactory<DefaultConsistentHash> chf = RendezvousConsistentHashFactory.getInstance();
      DefaultConsistentHash ch1 = chf.create(2, 64, Arrays.asList(A1, B1, C1), null);
      DefaultConsistentHash ch2 = chf.create(2, 64, Arrays.asList(A2, B2, C2), null);

      assertNotEquals(ch1, ch2, "Different UUIDs should (almost certainly) produce different CHs");
   }
}
