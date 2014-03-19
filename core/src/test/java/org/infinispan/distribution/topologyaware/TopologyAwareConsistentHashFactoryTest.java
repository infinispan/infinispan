package org.infinispan.distribution.topologyaware;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.2
 */
@Test(groups = "unit", testName = "distribution.topologyaware.TopologyAwareConsistentHashFactoryTest")
public class TopologyAwareConsistentHashFactoryTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(TopologyAwareConsistentHashFactoryTest.class);
   private static final int CLUSTER_SIZE = 10;
   public int numSegments = 100;

   private TestTopologyAwareAddress[] testAddresses;
   private List<Address> chMembers;
   private Map<Address, Float> capacityFactors;
   private ConsistentHashFactory<DefaultConsistentHash> chf;
   protected DefaultConsistentHash ch;

   @BeforeMethod()
   public void setUp() {
      chf = createConsistentHashFactory();
      chMembers = new ArrayList<Address>(CLUSTER_SIZE);
      capacityFactors = null;
      testAddresses = new TestTopologyAwareAddress[CLUSTER_SIZE];
      for (int i = 0; i < 10; i++) {
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
      capacityFactors = new HashMap<Address, Float>();
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
   }

   private void assertAllLocationsWithRebalance(int numOwners) {
      ch = chf.create(new MurmurHash3(), numOwners, numSegments, chMembers, capacityFactors);

      List<Address> membersWithLoad = computeNodesWithLoad(chMembers);
      assertAllLocations(numOwners, membersWithLoad);
      assertDistribution(numOwners, membersWithLoad);

      ch = chf.create(new MurmurHash3(), numOwners, numSegments, chMembers.subList(0, 1), capacityFactors);
      assertAllLocations(numOwners, chMembers.subList(0, 1));

      for (int i = 2; i <= chMembers.size(); i++) {
         List<Address> currentMembers = chMembers.subList(0, i);
         log.debugf("Created CH with numOwners %d, members %s", numOwners, currentMembers);
         ch = chf.updateMembers(ch, currentMembers, capacityFactors);
         ch = chf.rebalance(ch);

         membersWithLoad = computeNodesWithLoad(currentMembers);
         assertAllLocations(numOwners, membersWithLoad);
      }
   }

   private List<Address> computeNodesWithLoad(List<Address> nodes) {
      List<Address> membersWithLoad = new ArrayList<Address>(nodes.size());
      for (Address a : nodes) {
         if (capacityFactors == null || capacityFactors.get(a) > 0.0) {
            membersWithLoad.add(a);
         }
      }
      return membersWithLoad;
   }

   protected void assertDistribution(int numOwners, List<Address> currentMembers) {
      TopologyAwareOwnershipStatistics stats = new TopologyAwareOwnershipStatistics(ch);
      log.tracef("Ownership stats: %s", stats);
      for (Address node : currentMembers) {
         int expectedPrimarySegments = stats.computeExpectedSegments(numSegments, 1, node);
         int expectedOwnedSegments = stats.computeExpectedSegments(numSegments, numOwners, node);
         assertTrue(expectedPrimarySegments - 1 <= stats.getPrimaryOwned(node), "Too few primary segments for node " + node);
         assertTrue(stats.getPrimaryOwned(node) <= expectedPrimarySegments + 1, "Too many primary segments for node "
               + node);
         assertTrue(expectedOwnedSegments * 0.7 <= stats.getOwned(node), "Too few segments for node " + node);
         assertTrue(stats.getOwned(node) <= expectedOwnedSegments * 1.25, "Too many segments for node " + node);
      }
   }

   private int countMachines(List<Address> addresses) {
      Set<String> machines = new HashSet<String>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         machines.add(taa.getMachineId() + taa.getRackId() + taa.getSiteId());
      }
      return machines.size();
   }

   private int countRacks(List<Address> addresses) {
      Set<String> racks = new HashSet<String>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         racks.add(taa.getRackId() + taa.getSiteId());
      }
      return racks.size();
   }

   private int countSites(List<Address> addresses) {
      Set<String> sites = new HashSet<String>(addresses.size());
      for (Address a : addresses) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         sites.add(taa.getSiteId());
      }
      return sites.size();
   }

   private void assertAllLocations(int numOwners, List<Address> currentMembers) {
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
      assertAllLocations(numOwners, chMembers);
      assertDistribution(numOwners, chMembers);

      for (Address addr : chMembers) {
         log.debugf("Removing node %s" + addr);
         List<Address> addressCopy = new ArrayList<Address>(chMembers);
         addressCopy.remove(addr);
         DefaultConsistentHash newCH = chf.updateMembers(ch, addressCopy, null);
         newCH = chf.rebalance(newCH);

         // Allow a small number of segment moves, even though this is a leave, because the CH factory
         // generates extra moves trying to balance the CH.
         AtomicInteger movedSegmentsCount = new AtomicInteger(0);
         for (int segment = 0; segment < numSegments; segment++) {
            checkConsistency(segment, numOwners, ch.locateOwnersForSegment(segment), addr, newCH, movedSegmentsCount);
         }
         assert movedSegmentsCount.get() <= numOwners * numSegments * 0.1 :
               String.format("Too many moved segments after leave: %d. CH after leave is: %s\nPrevious: %s",
                     movedSegmentsCount.get(), newCH, ch);
      }
   }

   private void checkConsistency(int segment, int replCount, List<Address> originalOwners,
                                 Address removedAddress, DefaultConsistentHash newCH,
                                 AtomicInteger movedSegmentsCount) {
      List<Address> currentOwners = newCH.locateOwnersForSegment(segment);
      originalOwners = new ArrayList<Address>(originalOwners);
      originalOwners.remove(removedAddress);

      assertEquals(replCount, currentOwners.size(), currentOwners.toString());
      if (!currentOwners.containsAll(originalOwners))
         movedSegmentsCount.incrementAndGet();
   }


   private void assertSegmentLocation(int segment, int expectedOwners, int expectedMachines, int expectedRacks,
                                      int expectedSites) {
      List<Address> received = ch.locateOwnersForSegment(segment);

      // Check the number of addresses and uniqueness
      assertEquals(received.size(), expectedOwners);
      Set<Address> receivedUnique = new HashSet<Address>(received);
      assertEquals(receivedUnique.size(), expectedOwners);

      // Check the number of machines
      Set<String> receivedMachines = new HashSet<String>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedMachines.add(taa.getMachineId() + "|" + taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedMachines.size(), expectedMachines);

      // Check the number of racks
      Set<String> receivedRacks = new HashSet<String>();
      for (Address a : received) {
         TopologyAwareAddress taa = (TopologyAwareAddress) a;
         receivedRacks.add(taa.getRackId() + "|" + taa.getSiteId());
      }
      assertEquals(receivedRacks.size(), expectedRacks);

      // Check the number of sites
      Set<String> receivedSites = new HashSet<String>();
      for (Address a : received) {
         receivedSites.add(((TopologyAwareAddress) a).getSiteId());
      }
      assertEquals(receivedSites.size(), expectedSites);
   }

   private void addNode(TestTopologyAwareAddress address,
                        String machineId, String rackId, String siteId) {
      address.setSiteId(siteId);
      address.setRackId(rackId);
      address.setMachineId(machineId);
      chMembers.add(address);
   }

   protected void updateConsistentHash(int numOwners) {
      ch = chf.create(new MurmurHash3(), numOwners, numSegments, chMembers, null);
      log.debugf("Created CH with numOwners %d, members %s", numOwners, chMembers);
   }
}


class TopologyAwareOwnershipStatistics {
   private final DefaultConsistentHash ch;
   TopologyInfo topologyInfo;
   OwnershipStatistics stats;

   public TopologyAwareOwnershipStatistics(DefaultConsistentHash ch) {
      this.ch = ch;
      topologyInfo = new TopologyInfo(ch.getMembers(), ch.getCapacityFactors());
      stats = new OwnershipStatistics(ch, ch.getMembers());
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

   public int computeExpectedSegments(int numSegments, int numOwners, Address node) {
      return topologyInfo.computeExpectedSegments(numSegments, numOwners, node);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("TopologyAwareOwnershipStatistics{\n");
      for (String site : topologyInfo.getAllSites()) {
         sb.append(String.format("  %s: %d/%d\n", site, getSitePrimaryOwned(site), getSiteOwned(site)));
         for (String rack : topologyInfo.getSiteRacks(site)) {
            sb.append(String.format("    %s: %d/%d\n", rack, getRackPrimaryOwned(site, rack),
                  getRackOwned(site, rack)));
            for (String machine : topologyInfo.getRackMachines(site, rack)) {
               sb.append(String.format("      %s: %d/%d\n", machine,
                     getMachinePrimaryOwned(site, rack, machine),
                     getMachineOwned(site, rack, machine)));
               for (Address node : topologyInfo.getMachineNodes(site, rack, machine)) {
                  sb.append(String.format("        %s: %d/%d (%d)\n", node, stats.getPrimaryOwned(node),
                        stats.getOwned(node), topologyInfo.computeExpectedSegments(ch.getNumSegments(),
                        ch.getNumOwners(), node)));
               }
            }
         }
      }
      sb.append('}');
      return sb.toString();
   }
}
