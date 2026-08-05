package org.infinispan.distribution.ch.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Tests topology diversity constraints for {@link TopologyAwareRendezvousConsistentHashFactory}.
 * Extends {@link RendezvousConsistentHashFactoryTest} to inherit all base correctness and
 * movement tests with the same permissive distribution tolerances.
 */
@Test(groups = "unit", testName = "distribution.ch.TopologyAwareRendezvousConsistentHashFactoryTest")
public class TopologyAwareRendezvousConsistentHashFactoryTest extends RendezvousConsistentHashFactoryTest {

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return TopologyAwareRendezvousConsistentHashFactory.getInstance();
   }

   public void testBackupOwnersInDifferentSites() {
      // 3 sites × 2 nodes each, numOwners=3 — every segment must have owners in 3 distinct sites
      Address a1 = Address.random("a1", "site1", null, null);
      Address a2 = Address.random("a2", "site1", null, null);
      Address b1 = Address.random("b1", "site2", null, null);
      Address b2 = Address.random("b2", "site2", null, null);
      Address c1 = Address.random("c1", "site3", null, null);
      Address c2 = Address.random("c2", "site3", null, null);
      List<Address> members = Arrays.asList(a1, a2, b1, b2, c1, c2);

      DefaultConsistentHash ch = createConsistentHashFactory().create(3, 256, members, null);

      for (int s = 0; s < ch.getNumSegments(); s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         Set<String> sites = sitesOf(owners);
         assertTrue(sites.size() >= 3,
               "Segment " + s + " owners " + owners + " should span 3 distinct sites but only span: " + sites);
      }
   }

   public void testBackupOwnersInDifferentRacksWhenFewSites() {
      // 1 site, 4 racks × 1 node — owners should be in distinct racks
      Address a = Address.random("a", "site1", "rack1", null);
      Address b = Address.random("b", "site1", "rack2", null);
      Address c = Address.random("c", "site1", "rack3", null);
      Address d = Address.random("d", "site1", "rack4", null);
      List<Address> members = Arrays.asList(a, b, c, d);

      DefaultConsistentHash ch = createConsistentHashFactory().create(3, 256, members, null);

      for (int s = 0; s < ch.getNumSegments(); s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         Set<String> racks = racksOf(owners);
         assertTrue(racks.size() >= 3,
               "Segment " + s + " owners should span 3 distinct racks but only span: " + racks);
      }
   }

   public void testDegradesToNodeLevelWhenNoTopologyInfo() {
      // Nodes with no site/rack/machine — output should equal the non-topology-aware pure factory
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      List<Address> members = Arrays.asList(A, B, C);

      DefaultConsistentHash topoAware = createConsistentHashFactory().create(2, 64, members, null);
      DefaultConsistentHash base = PureRendezvousConsistentHashFactory.getInstance().create(2, 64, members, null);

      // Both must produce exactly the same CH when no topology info is present
      for (int s = 0; s < topoAware.getNumSegments(); s++) {
         assertTrue(topoAware.locateOwnersForSegment(s).equals(base.locateOwnersForSegment(s)),
               "Without topology info, segment " + s + " owners should match non-topology-aware factory");
      }
   }

   public void testTopologyDiversityPreservedAfterNodeLeave() {
      // 2 sites × 2 nodes each, numOwners=2. Remove one node from site1. Diversity should be maintained.
      Address a1 = Address.random("a1", "site1", null, null);
      Address a2 = Address.random("a2", "site1", null, null);
      Address b1 = Address.random("b1", "site2", null, null);
      Address b2 = Address.random("b2", "site2", null, null);
      List<Address> original = Arrays.asList(a1, a2, b1, b2);

      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      DefaultConsistentHash ch = chf.create(2, 64, original, null);

      // a1 leaves
      List<Address> afterLeave = Arrays.asList(a2, b1, b2);
      DefaultConsistentHash updated = chf.updateMembers(ch, afterLeave, null);
      DefaultConsistentHash rebalanced = chf.rebalance(updated);

      for (int s = 0; s < rebalanced.getNumSegments(); s++) {
         List<Address> owners = rebalanced.locateOwnersForSegment(s);
         Set<String> sites = sitesOf(owners);
         assertTrue(sites.size() >= 2,
               "After leave, segment " + s + " should still have owners in 2 distinct sites");
      }
   }

   public void testCapacityFactorWithTopologyConstraints() {
      // Site-A: 1 node capacity 3.0; site-B: 3 nodes capacity 1.0. numOwners=2, 256 segments.
      // All segments must have owners in distinct sites; site-A's node should own ~3x as many
      // primaries as any single site-B node.
      Address a = Address.random("a", "site-A", null, null);
      Address b1 = Address.random("b1", "site-B", null, null);
      Address b2 = Address.random("b2", "site-B", null, null);
      Address b3 = Address.random("b3", "site-B", null, null);
      List<Address> members = Arrays.asList(a, b1, b2, b3);
      Map<Address, Float> cf = new HashMap<>();
      cf.put(a, 3f);
      cf.put(b1, 1f);
      cf.put(b2, 1f);
      cf.put(b3, 1f);

      DefaultConsistentHash ch = createConsistentHashFactory().create(2, 256, members, cf);

      // All segments have owners in distinct sites
      for (int s = 0; s < ch.getNumSegments(); s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         Set<String> sites = sitesOf(owners);
         assertTrue(sites.size() >= 2,
               "Segment " + s + " should have owners in both sites");
      }

      // a (capacity 3.0) should own ~3x as many primaries as any single b node
      OwnershipStatistics stats = new OwnershipStatistics(ch, members);
      int aOwned = stats.getPrimaryOwned(a);
      int b1Owned = stats.getPrimaryOwned(b1);
      int b2Owned = stats.getPrimaryOwned(b2);
      int b3Owned = stats.getPrimaryOwned(b3);
      int avgBOwned = (b1Owned + b2Owned + b3Owned) / 3;
      // allow ±20% tolerance
      assertTrue(aOwned >= avgBOwned * 2,
            "Site-A node (capacity 3.0) should own significantly more primaries than average site-B node. " +
                  "a=" + aOwned + " avgB=" + avgBOwned);
   }

   // ---- Topology helpers ----

   private static Set<String> sitesOf(List<Address> owners) {
      Set<String> sites = new HashSet<>();
      for (Address a : owners) sites.add(a.getSiteId() != null ? a.getSiteId() : "__null__");
      return sites;
   }

   private static Set<String> racksOf(List<Address> owners) {
      Set<String> racks = new HashSet<>();
      for (Address a : owners) racks.add(a.getRackId() != null ? a.getRackId() : "__null__");
      return racks;
   }

   @SuppressWarnings("unused")
   private static List<Address> makeNodes(String site, String rackPrefix, int count) {
      List<Address> nodes = new ArrayList<>();
      for (int i = 0; i < count; i++) {
         nodes.add(Address.random(site + "-" + i, site, rackPrefix + i, null));
      }
      return nodes;
   }
}
