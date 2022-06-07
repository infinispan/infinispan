package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.SyncConsistentHashFactory.Builder.fudgeExpectedSegments;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.distribution.Member;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.testng.annotations.Test;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link SyncConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.SyncConsistentHashFactoryTest")
public class SyncConsistentHashFactoryTest extends DefaultConsistentHashFactoryTest {

   private final PersistentUUIDManager uuidManager = new PersistentUUIDManagerImpl();

   @Override
   protected SyncConsistentHashFactory createConsistentHashFactory() {
      return new SyncConsistentHashFactory();
   }

   @Override
   protected TestAddress createAddress(int num, String name) {
      TestAddress address = super.createAddress(num, name);
      PersistentUUID uuid = uuidManager.getPersistentUuid(address);
      if (uuid == null) {
         uuidManager.addPersistentAddressMapping(address, PersistentUUID.randomUUID());
      }

      return address;
   }

   @Override
   protected List<Member> toMembers(List<Address> addresses, Map<Address, Float> capacities) {
      List<Member> members = new ArrayList<>();
      for (Address address: addresses) {
         float factor = capacities != null ? capacities.getOrDefault(address, 1.0f) : 1.0f;
         Member member = new Member(address, uuidManager.getPersistentUuid(address), factor);
         members.add(member);
      }

      return members;
   }

   // Disclaimer: These numbers just happen to work with our test addresses, they are by no means guaranteed
   // by the SyncConsistentHashFactory algorithm. In theory it could trade stability of segments on join/leave
   // in order to guarantee a better distribution, but I haven't done anything in that area yet.
   protected float maxOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      if (expectedOwned == 0)
         return 0;

      float averageOwned = 1f * numSegments * actualNumOwners / numNodes;
      float maxDiff;
      if (expectedOwned >= averageOwned) {
         maxDiff = .18f * expectedOwned;
      } else {
         maxDiff = .10f * (expectedOwned + averageOwned);
      }
      return expectedOwned + Math.max(maxDiff, 1);
   }

   protected float minOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      if (expectedOwned == 0)
         return 0;

      float averageOwned = 1f * numSegments * actualNumOwners / numNodes;
      float maxDiff;
      if (expectedOwned >= averageOwned) {
         maxDiff = .18f * expectedOwned;
      } else {
         maxDiff = .10f * (expectedOwned + averageOwned);
      }
      return expectedOwned - Math.max(maxDiff, 1);
   }

   @Override
   protected float allowedExtraMoves(ConsistentHash oldCH, ConsistentHash newCH,
                                     int joinerSegments, int leaverSegments) {
      int oldSize = nodesWithLoad(oldCH.getMembers(), oldCH.getCapacityFactors());
      int newSize = nodesWithLoad(newCH.getMembers(), newCH.getCapacityFactors());
      int maxSize = Math.max(oldSize, newSize);
      return Math.max(maxSize, 0.22f * newCH.getNumOwners() * newCH.getNumSegments());
   }

   public void testFudgeExpectedSegments() {
      float averageSegments = 10;
      assertEquals(0, fudgeExpectedSegments(0.1f, averageSegments, 0));
      assertEquals(0, fudgeExpectedSegments(0.1f, averageSegments, 1));
      assertEquals(0, fudgeExpectedSegments(0.1f, averageSegments, 2));
      assertEquals(0, fudgeExpectedSegments(0.1f, averageSegments, 3));
      assertEquals(1, fudgeExpectedSegments(0.1f, averageSegments, 4));
      assertEquals(2, fudgeExpectedSegments(0.1f, averageSegments, 5));

      assertEquals(0, fudgeExpectedSegments(0.9f, averageSegments, 0));
      assertEquals(0, fudgeExpectedSegments(0.9f, averageSegments, 1));
      assertEquals(0, fudgeExpectedSegments(0.9f, averageSegments, 2));
      assertEquals(1, fudgeExpectedSegments(0.9f, averageSegments, 3));
      assertEquals(2, fudgeExpectedSegments(0.9f, averageSegments, 4));

      assertEquals(0, fudgeExpectedSegments(1.4f, averageSegments, 0));
      assertEquals(0, fudgeExpectedSegments(1.4f, averageSegments, 1));
      assertEquals(0, fudgeExpectedSegments(1.4f, averageSegments, 2));
      assertEquals(1, fudgeExpectedSegments(1.4f, averageSegments, 3));
      assertEquals(2, fudgeExpectedSegments(1.4f, averageSegments, 4));

      assertEquals(0, fudgeExpectedSegments(1.6f, averageSegments, 0));
      assertEquals(0, fudgeExpectedSegments(1.6f, averageSegments, 1));
      assertEquals(1, fudgeExpectedSegments(1.6f, averageSegments, 2));
      assertEquals(2, fudgeExpectedSegments(1.6f, averageSegments, 3));
      assertEquals(3, fudgeExpectedSegments(1.6f, averageSegments, 4));

      assertEquals(1, fudgeExpectedSegments(4.4f, averageSegments, 0));
      assertEquals(2, fudgeExpectedSegments(4.4f, averageSegments, 1));
      assertEquals(3, fudgeExpectedSegments(4.4f, averageSegments, 2));
      assertEquals(4, fudgeExpectedSegments(4.4f, averageSegments, 3));
      assertEquals(5, fudgeExpectedSegments(4.4f, averageSegments, 4));

      assertEquals(2, fudgeExpectedSegments(4.6f, averageSegments, 0));
      assertEquals(3, fudgeExpectedSegments(4.6f, averageSegments, 1));
      assertEquals(4, fudgeExpectedSegments(4.6f, averageSegments, 2));
      assertEquals(5, fudgeExpectedSegments(4.6f, averageSegments, 3));
      assertEquals(6, fudgeExpectedSegments(4.6f, averageSegments, 4));

      assertEquals(7, fudgeExpectedSegments(10f, averageSegments, 0));
      assertEquals(8, fudgeExpectedSegments(10f, averageSegments, 1));
      assertEquals(9, fudgeExpectedSegments(10f, averageSegments, 2));
      assertEquals(10, fudgeExpectedSegments(10f, averageSegments, 3));
      assertEquals(11, fudgeExpectedSegments(10f, averageSegments, 4));

      assertEquals(97, fudgeExpectedSegments(100f, averageSegments, 0));
      assertEquals(98, fudgeExpectedSegments(100f, averageSegments, 1));
      assertEquals(99, fudgeExpectedSegments(100f, averageSegments, 2));
      assertEquals(100, fudgeExpectedSegments(100f, averageSegments, 3));
      assertEquals(101, fudgeExpectedSegments(100f, averageSegments, 4));

      assertEquals(997, fudgeExpectedSegments(1000f, averageSegments, 0));
      assertEquals(998, fudgeExpectedSegments(1000f, averageSegments, 1));
      assertEquals(999, fudgeExpectedSegments(1000f, averageSegments, 2));
      assertEquals(1000, fudgeExpectedSegments(1000f, averageSegments, 3));
      assertEquals(1001, fudgeExpectedSegments(1000f, averageSegments, 4));
   }
}
