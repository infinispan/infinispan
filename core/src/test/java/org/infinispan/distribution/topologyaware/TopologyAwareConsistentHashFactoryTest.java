package org.infinispan.distribution.topologyaware;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestTopologyAwareAddress;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.distribution.ch.impl.TopologyAwareConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.2
 */
@Test(groups = "unit", testName = "distribution.topologyaware.TopologyAwareConsistentHashFactoryTest")
public class TopologyAwareConsistentHashFactoryTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(TopologyAwareConsistentHashFactoryTest.class);
   private static final int ADDRESS_COUNT = 25;
   public int numSegments = 100;

   private TestTopologyAwareAddress[] testAddresses;
   private List<Address> chMembers;
   private Map<Address, Float> capacityFactors;
   private ConsistentHashFactory<DefaultConsistentHash> chf;
   protected DefaultConsistentHash ch;

   @BeforeMethod()
   public void setUp() {
      chf = createConsistentHashFactory();
      chMembers = new ArrayList<>(ADDRESS_COUNT);
      capacityFactors = null;
      testAddresses = new TestTopologyAwareAddress[ADDRESS_COUNT];
      for (int i = 0; i < ADDRESS_COUNT; i++) {
         testAddresses[i] = new TestTopologyAwareAddress(i * 100);
         testAddresses[i].setName(Character.toString((char) ('A' + i)));
      }
   }

   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new TopologyAwareConsistentHashFactory();
   }

   public void testNumberOfOwners() {
      addNode(testAddresses[0], "m0", null, null);

      updateConsistentHash(1);
      assertEquals(ch.locateOwners(testAddresses[0]).size(), 1);
      updateConsistentHash(2);
      assertEquals(ch.locateOwners(testAddresses[0]).size(), 1);

      addNode(testAddresses[1], "m1", null, null);

      updateConsistentHash(1);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 1);
      }
      updateConsistentHash(2);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }
      updateConsistentHash(3);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }

      addNode(testAddresses[2], "m0", null, null);

      updateConsistentHash(1);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 1);
      }
      updateConsistentHash(2);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 2);
      }
      updateConsistentHash(3);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 3);
      }
      updateConsistentHash(4);
      for (Address testAddress : testAddresses) {
         assertEquals(ch.locateOwners(testAddress).size(), 3);
      }
   }

   public void testDifferentMachines() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m1", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m1", null, null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
   }

   public void testNumOwnerBiggerThanAvailableNodes() {
      // test first with one node
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m0", null, null);

      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
      assertAllLocationsWithRebalance(99);
   }

   public void testDifferentMachines2() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m1", null, null);
      addNode(testAddresses[3], "m1", null, null);
      addNode(testAddresses[4], "m2", null, null);
      addNode(testAddresses[5], "m2", null, null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testDifferentMachines3() {
      addNode(testAddresses[0], "m0", "r1", "s1");
      addNode(testAddresses[1], "m1", "r1", "s1");
      addNode(testAddresses[2], "m2", "r1", "s1");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testDifferentRacksAndMachines() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m2", "r1", null);
      addNode(testAddresses[5], "m2", "r2", null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testAllSameMachine() {
      addNode(testAddresses[0], "m0", null, null);
      addNode(testAddresses[1], "m0", null, null);
      addNode(testAddresses[2], "m0", null, null);
      addNode(testAddresses[3], "m0", null, null);
      addNode(testAddresses[4], "m0", null, null);
      addNode(testAddresses[5], "m0", null, null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testDifferentSites() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s0");
      addNode(testAddresses[2], "m2", null, "s1");
      addNode(testAddresses[3], "m3", null, "s1");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testSitesMachines2() {
      addNode(testAddresses[0], "m0", null, "s0");
      addNode(testAddresses[1], "m1", null, "s1");
      addNode(testAddresses[2], "m2", null, "s0");
      addNode(testAddresses[3], "m3", null, "s2");
      addNode(testAddresses[4], "m4", null, "s1");
      addNode(testAddresses[5], "m5", null, "s1");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testSitesMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", null, "r0");
      addNode(testAddresses[1], "m0", null, "r1");
      addNode(testAddresses[2], "m0", null, "r0");
      addNode(testAddresses[3], "m0", null, "r2");
      addNode(testAddresses[4], "m0", null, "r1");
      addNode(testAddresses[5], "m0", null, "r1");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testDifferentRacks() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r0", null);
      addNode(testAddresses[2], "m2", "r1", null);
      addNode(testAddresses[3], "m3", "r1", null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testRacksMachines2() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m1", "r1", null);
      addNode(testAddresses[2], "m2", "r0", null);
      addNode(testAddresses[3], "m3", "r2", null);
      addNode(testAddresses[4], "m4", "r1", null);
      addNode(testAddresses[5], "m5", "r1", null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testRacksMachinesSameMachineName() {
      addNode(testAddresses[0], "m0", "r0", null);
      addNode(testAddresses[1], "m0", "r1", null);
      addNode(testAddresses[2], "m0", "r0", null);
      addNode(testAddresses[3], "m0", "r2", null);
      addNode(testAddresses[4], "m0", "r1", null);
      addNode(testAddresses[5], "m0", "r1", null);

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testComplexScenario() {
      // {s0: {r0: {m0, m1}}, s1: {r0: {m0, m1, m2}, r1: {m0}}}
      addNode(testAddresses[0], "m2", "r0", "s1");
      addNode(testAddresses[1], "m1", "r0", "s0");
      addNode(testAddresses[2], "m1", "r0", "s1");
      addNode(testAddresses[3], "m1", "r1", "s0");
      addNode(testAddresses[4], "m0", "r0", "s1");
      addNode(testAddresses[5], "m0", "r1", "s1");
      addNode(testAddresses[6], "m0", "r1", "s0");
      addNode(testAddresses[7], "m0", "r0", "s1");
      addNode(testAddresses[8], "m0", "r0", "s0");
      addNode(testAddresses[9], "m0", "r0", "s0");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
      assertAllLocationsWithRebalance(3);
      assertAllLocationsWithRebalance(4);
   }

   public void testComplexScenario2() {
      // {s0: {r0: {m0, m1, m2}, r1: {m3, m4, m5}, r1: {m6, m7, m8}}}
      addNode(testAddresses[0], "m0", "r0", "s0");
      addNode(testAddresses[1], "m1", "r0", "s0");
      addNode(testAddresses[2], "m2", "r0", "s0");
      addNode(testAddresses[3], "m3", "r1", "s0");
      addNode(testAddresses[4], "m4", "r1", "s0");
      addNode(testAddresses[5], "m5", "r1", "s0");
      addNode(testAddresses[6], "m6", "r2", "s0");
      addNode(testAddresses[7], "m7", "r2", "s0");
      addNode(testAddresses[8], "m8", "r2", "s0");

      assertAllLocationsWithRebalance(1);
      assertAllLocationsWithRebalance(2);
   }

   public void testLoadFactors() {
      try {
         capacityFactors = new HashMap<>();
         capacityFactors.put(testAddresses[0], 2.0f);
         capacityFactors.put(testAddresses[1], 0.0f);
         capacityFactors.put(testAddresses[2], 1.0f);
         capacityFactors.put(testAddresses[3], 2.0f);
         capacityFactors.put(testAddresses[4], 0.0f);
         capacityFactors.put(testAddresses[5], 1.0f);
         capacityFactors.put(testAddresses[6], 2.0f);
         capacityFactors.put(testAddresses[7], 0.0f);
         capacityFactors.put(testAddresses[8], 1.0f);

         // {s0: {r0: {m0, m1, m2}, r1: {m3, m4, m5}, r1: {m6, m7, m8}}}
         addNode(testAddresses[0], "m0", "r0", "s0");
         addNode(testAddresses[1], "m1", "r0", "s0");
         addNode(testAddresses[2], "m2", "r0", "s0");
         addNode(testAddresses[3], "m3", "r1", "s0");
         addNode(testAddresses[4], "m4", "r1", "s0");
         addNode(testAddresses[5], "m5", "r1", "s0");
         addNode(testAddresses[6], "m6", "r2", "s0");
         addNode(testAddresses[7], "m7", "r2", "s0");
         addNode(testAddresses[8], "m8", "r2", "s0");
         assertAllLocationsWithRebalance(1);
         assertAllLocationsWithRebalance(2);
         assertAllLocationsWithRebalance(3);
      } finally {
         capacityFactors = null;
      }
   }

   private void assertAllLocationsWithRebalance(int numOwners) {
      ch = chf.create(MurmurHash3.getInstance(), numOwners, numSegments, chMembers, capacityFactors);

      List<Address> membersWithLoad = computeNodesWithLoad(chMembers);
      assertAllLocations(membersWithLoad, numOwners);
      assertDistribution(membersWithLoad, numOwners);

      ch = chf.create(MurmurHash3.getInstance(), numOwners, numSegments, chMembers.subList(0, 1), capacityFactors);
      assertAllLocations(chMembers.subList(0, 1), numOwners);

      for (int i = 2; i <= chMembers.size(); i++) {
         List<Address> currentMembers = chMembers.subList(0, i);
         log.debugf("Created CH with numOwners %d, members %s", numOwners, currentMembers);
         ch = chf.updateMembers(ch, currentMembers, capacityFactors);
         ch = chf.rebalance(ch);

         membersWithLoad = computeNodesWithLoad(currentMembers);
         assertAllLocations(membersWithLoad, numOwners);
      }
   }

   private List<Address> computeNodesWithLoad(List<Address> nodes) {
      List<Address> membersWithLoad = new ArrayList<>(nodes.size());
      for (Address a : nodes) {
         if (capacityFactors == null || capacityFactors.get(a) > 0.0) {
            membersWithLoad.add(a);
         }
      }
      return membersWithLoad;
   }

   protected void assertDistribution(List<Address> currentMembers, int numOwners) {
      assertDistribution(currentMembers, numOwners, numSegments);
   }

   protected void assertDistribution(List<Address> currentMembers, int numOwners, int numSegments) {
      TopologyAwareOwnershipStatistics stats = new TopologyAwareOwnershipStatistics(ch);
      log.tracef("Ownership stats: %s", stats);
      for (Address node : currentMembers) {
         float expectedPrimarySegments = stats.computeExpectedPrimarySegments(node);
         float expectedOwnedSegments = stats.computeExpectedOwnedSegments(node);
         int owned = stats.getOwned(node);
         int primaryOwned = stats.getPrimaryOwned(node);
         assertTrue(expectedPrimarySegments - 1 <= primaryOwned,
                    "Too few primary segments for node " + node);
         assertTrue(primaryOwned <= expectedPrimarySegments + 1,
                    "Too many primary segments for node " + node);
         assertTrue(Math.floor(expectedOwnedSegments * 0.7) <= owned,
                    "Too few segments for node " + node);
         assertTrue(owned <= Math.ceil(expectedOwnedSegments * 1.25),
                    "Too many segments for node " + node);
      }
   }

   private int countMachines(List<Address> addresses) {
      Set<String> machines = new HashSet<>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         machines.add(taa.getMachineId() + taa.getRackId() + taa.getSiteId());
      }
      return machines.size();
   }

   private int countRacks(List<Address> addresses) {
      Set<String> racks = new HashSet<>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         racks.add(taa.getRackId() + taa.getSiteId());
      }
      return racks.size();
   }

   private int countSites(List<Address> addresses) {
      Set<String> sites = new HashSet<>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         sites.add(taa.getSiteId());
      }
      return sites.size();
   }

   private void assertAllLocations(List<Address> currentMembers, int numOwners) {
      assertAllLocations(currentMembers, numOwners, numSegments);
   }

   private void assertAllLocations(List<Address> currentMembers, int numOwners, int numSegments) {
      int expectedOwners = Math.min(numOwners, currentMembers.size());
      int expectedMachines = Math.min(expectedOwners, countMachines(currentMembers));
      int expectedRacks = Math.min(expectedOwners, countRacks(currentMembers));
      int expectedSites = Math.min(expectedOwners, countSites(currentMembers));

      for (int segment = 0; segment < numSegments; segment++) {
         assertSegmentLocation(segment, expectedOwners, expectedMachines, expectedRacks, expectedSites);
      }
   }

   public void testConsistencyWhenNodeLeaves() {
      addNode(testAddresses[0], "m2", "r0", "s1");
      addNode(testAddresses[1], "m1", "r0", "s0");
      addNode(testAddresses[2], "m1", "r0", "s1");
      addNode(testAddresses[3], "m1", "r1", "s0");
      addNode(testAddresses[4], "m0", "r0", "s1");
      addNode(testAddresses[5], "m0", "r1", "s1");
      addNode(testAddresses[6], "m0", "r1", "s0");
      addNode(testAddresses[7], "m0", "r0", "s3");
      addNode(testAddresses[8], "m0", "r0", "s2");
      addNode(testAddresses[9], "m0", "r0", "s0");

      int numOwners = 3;
      updateConsistentHash(numOwners);
      assertAllLocations(chMembers, numOwners);
      assertDistribution(chMembers, numOwners);

      for (Address addr : chMembers) {
         log.debugf("Removing node %s", addr);
         List<Address> addressCopy = new ArrayList<>(chMembers);
         addressCopy.remove(addr);
         DefaultConsistentHash newCH = chf.updateMembers(ch, addressCopy, null);
         newCH = chf.rebalance(newCH);

         // Allow a small number of segment moves, even though this is a leave, because the CH factory
         // generates extra moves trying to balance the CH.
         AtomicInteger movedSegmentsCount = new AtomicInteger(0);
         for (int segment = 0; segment < numSegments; segment++) {
            checkConsistency(segment, numOwners, addr, newCH, movedSegmentsCount);
         }
         assert movedSegmentsCount.get() <= numSegments * numOwners * 0.1 :
               String.format("Too many moved segments after leave: %d. CH after leave is: %s\nPrevious: %s",
                     movedSegmentsCount.get(), newCH, ch);
      }
   }

   private void checkConsistency(int segment, int replCount, Address removedAddress,
         DefaultConsistentHash newCH, AtomicInteger movedSegmentsCount) {
      List<Address> removedOwners = new ArrayList<>(ch.locateOwnersForSegment(segment));
      List<Address> currentOwners = newCH.locateOwnersForSegment(segment);
      removedOwners.remove(removedAddress);
      removedOwners.removeAll(currentOwners);

      assertEquals(replCount, currentOwners.size(), currentOwners.toString());
      if (!currentOwners.containsAll(removedOwners))
         movedSegmentsCount.addAndGet(removedOwners.size());
   }

   private void assertSegmentLocation(int segment, int expectedOwners, int expectedMachines, int expectedRacks,
                                      int expectedSites) {
      List<Address> received = ch.locateOwnersForSegment(segment);

      // Check the number of addresses and uniqueness
      assertEquals(received.size(), expectedOwners);
      Set<Address> receivedUnique = new HashSet<>(received);
      assertEquals(receivedUnique.size(), expectedOwners);

      // Check the number of machines
      Set<String> receivedMachines = new HashSet<>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedMachines.add(taa.getMachineId() + "|" + taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedMachines.size(), expectedMachines);

      // Check the number of racks
      Set<String> receivedRacks = new HashSet<>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedRacks.add(taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedRacks.size(), expectedRacks);

      // Check the number of sites
      Set<String> receivedSites = new HashSet<>();
      for (Address a : received) {
         receivedSites.add(((TopologyAwareAddress) a).getSiteId());
      }
      assertEquals(receivedSites.size(), expectedSites);
   }

   private void addNode(TestTopologyAwareAddress address,
                        String machineId, String rackId, String siteId) {
      address.setSiteId(siteId);
      address.setRackId(siteId + rackId);
      address.setMachineId(siteId + rackId + machineId);
      chMembers.add(address);
   }

   protected void updateConsistentHash(int numOwners) {
      updateConsistentHash(numOwners, numSegments);
   }

   private void updateConsistentHash(int numOwners, int numSegments) {
      ch = chf.create(MurmurHash3.getInstance(), numOwners, numSegments, chMembers, capacityFactors);
      log.debugf("Created CH with numOwners %d, members %s", numOwners, chMembers);
   }

   @Test(timeOut = 10000)
   public void testSmallNumberOfSegments() {
      for (int i = 0; i < 3; i++) {
         addNode(testAddresses[i], "m0", "r0", "s0");
      }

      updateConsistentHash(2, 1);
      assertAllLocations(chMembers, 2, 1);
      assertDistribution(chMembers, 2, 1);

      for (int i = 3; i < ADDRESS_COUNT; i++) {
         addNode(testAddresses[i], "m0", "r0", "s0");
      }

      updateConsistentHash(2, 256);
      assertAllLocations(chMembers, 2, 1);
      assertDistribution(chMembers, 2, 1);
   }
}


class TopologyAwareOwnershipStatistics {
   TopologyInfo topologyInfo;
   OwnershipStatistics stats;
   private int numSegments;
   private int numOwners;

   public TopologyAwareOwnershipStatistics(DefaultConsistentHash ch) {
      numSegments = ch.getNumSegments();
      numOwners = ch.getNumOwners();
      topologyInfo = new TopologyInfo(numSegments, numOwners, ch.getMembers(), ch.getCapacityFactors());
      stats = new OwnershipStatistics(ch, ch.getMembers());
   }

   public TopologyAwareOwnershipStatistics(TopologyInfo topologyInfo, OwnershipStatistics stats, int numSegments, int numOwners) {
      this.topologyInfo = topologyInfo;
      this.stats = stats;
      this.numSegments = numSegments;
      this.numOwners = numOwners;
   }

   public int getSiteOwned(String site) {
      int count = 0;
      for (Address node : topologyInfo.getSiteNodes(site)) {
         count += stats.getOwned(node);
      }
      return count;
   }

   public int getSitePrimaryOwned(String site) {
      int count = 0;
      for (Address node : topologyInfo.getSiteNodes(site)) {
         count += stats.getPrimaryOwned(node);
      }
      return count;
   }

   public int getRackOwned(String site, String rack) {
      int count = 0;
      for (Address node : topologyInfo.getRackNodes(site, rack)) {
         count += stats.getOwned(node);
      }
      return count;
   }

   public int getRackPrimaryOwned(String site, String rack) {
      int count = 0;
      for (Address node : topologyInfo.getRackNodes(site, rack)) {
         count += stats.getPrimaryOwned(node);
      }
      return count;
   }

   public int getMachineOwned(String site, String rack, String machine) {
      int count = 0;
      for (Address node : topologyInfo.getMachineNodes(site, rack, machine)) {
         count += stats.getOwned(node);
      }
      return count;
   }

   public int getMachinePrimaryOwned(String site, String rack, String machine) {
      int count = 0;
      for (Address node : topologyInfo.getMachineNodes(site, rack, machine)) {
         count += stats.getPrimaryOwned(node);
      }
      return count;
   }

   public int getOwned(Address node) {
      return stats.getOwned(node);
   }

   public int getPrimaryOwned(Address node) {
      return stats.getPrimaryOwned(node);
   }

   public float computeExpectedPrimarySegments(Address node) {
      return topologyInfo.getExpectedPrimarySegments(node);
   }

   public float computeExpectedOwnedSegments(Address node) {
      return topologyInfo.getExpectedOwnedSegments(node);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("TopologyAwareOwnershipStatistics{\n");
      int clusterPrimaryOwned = stats.sumPrimaryOwned();
      sb.append(String.format("cluster: %d+%d\n", clusterPrimaryOwned, stats.sumOwned() - clusterPrimaryOwned));
      for (String site : topologyInfo.getAllSites()) {
         int sitePrimaryOwned = getSitePrimaryOwned(site);
         sb.append(String.format("  %s: %d+%d\n", site, sitePrimaryOwned, getSiteOwned(site) - sitePrimaryOwned));
         for (String rack : topologyInfo.getSiteRacks(site)) {
            int rackPrimaryOwned = getRackPrimaryOwned(site, rack);
            sb.append(String.format("    %s: %d+%d\n", rack, rackPrimaryOwned,
                                    getRackOwned(site, rack) - rackPrimaryOwned));
            for (String machine : topologyInfo.getRackMachines(site, rack)) {
               int machinePrimaryOwned = getMachinePrimaryOwned(site, rack, machine);
               sb.append(String.format("      %s: %d+%d\n", machine, machinePrimaryOwned,
                                       getMachineOwned(site, rack, machine) - machinePrimaryOwned));
               for (Address node : topologyInfo.getMachineNodes(site, rack, machine)) {
                  int nodePrimaryOwned = stats.getPrimaryOwned(node);
                  sb.append(String.format("        %s: %d+%d\n", node, nodePrimaryOwned,
                                          getOwned(node) - nodePrimaryOwned));
               }
            }
         }
      }
      sb.append('}');
      return sb.toString();
   }
}
