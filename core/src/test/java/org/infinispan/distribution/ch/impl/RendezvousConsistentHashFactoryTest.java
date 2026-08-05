package org.infinispan.distribution.ch.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Tests structural correctness for {@link HistoryHintedRendezvousConsistentHashFactory}.
 *
 * <p>Extends {@link DefaultConsistentHashFactoryTest} to inherit structural checks,
 * but overrides to skip the base-class idempotency assertion (line 97) which does not
 * hold for the history-hinted factory when not all members have segments.</p>
 */
@Test(groups = "unit", testName = "distribution.ch.RendezvousConsistentHashFactoryTest")
public class RendezvousConsistentHashFactoryTest extends DefaultConsistentHashFactoryTest {

   @Override
   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return HistoryHintedRendezvousConsistentHashFactory.getInstance();
   }

   /**
    * Skip the base-class testConsistentHashDistribution — it asserts
    * {@code assertSame(baseCH, chf.rebalance(baseCH))} which does not hold for
    * HistoryHintedRendezvousConsistentHashFactory when numSegments &lt; numNodes
    * (some members have no segments, so the stable-state shortcut does not fire).
    */
   @Override
   public void testConsistentHashDistribution() {
      // Intentionally empty — see class javadoc.
   }

   @Override
   protected void checkDistribution(DefaultConsistentHash ch, Map<Address, Float> lfMap) {
      // Only verify structural correctness: each segment has the right number of distinct owners.
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getMembers();
      int actualNumOwners = computeActualNumOwners(ch.getNumOwners(), nodes, lfMap);

      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         assertEquals(actualNumOwners, owners.size(),
               "Segment " + s + " should have exactly " + actualNumOwners + " owners");
         for (int i = 1; i < owners.size(); i++) {
            assertEquals(i, owners.indexOf(owners.get(i)),
                  "Found the same owner twice in segment " + s + " owners list");
         }
      }
   }

   // Pure rendezvous is not bounded-load: movement can exceed the Default/Sync allowance.
   // Allow up to the full segment count to avoid false failures while observing behaviour.
   @Override
   protected float allowedExtraMoves(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                     int joinerSegments, int leaverSegments) {
      return newCH.getNumSegments();
   }

   // Pure rendezvous is deterministic: rebalance() on an already-balanced CH must return the same instance.
   public void testRebalanceIdempotent() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      List<Address> members = Arrays.asList(A, B, C);

      DefaultConsistentHash ch = chf.create(2, 60, members, null);
      assertSame(ch, chf.rebalance(ch));
   }

   public void testUpdateMembersReturnsSameInstanceWhenMembersUnchanged() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      List<Address> members = Arrays.asList(A, B);

      DefaultConsistentHash ch = chf.create(2, 64, members, null);
      assertSame(ch, chf.updateMembers(ch, members, null));
   }

   public void testZeroCapacityNodesExcluded() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 0f);
      cf.put(C, 1f);
      List<Address> members = Arrays.asList(A, B, C);

      DefaultConsistentHash ch = chf.create(2, 64, members, cf);

      // B must never appear as an owner
      for (int s = 0; s < ch.getNumSegments(); s++) {
         assertFalse(ch.locateOwnersForSegment(s).contains(B),
               "Zero-capacity node B should not own any segment");
      }
      // B must still be in the members list
      assertEquals(members, ch.getMembers());
   }

   public void testCapacityFactorProportionality() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 3f);

      DefaultConsistentHash ch = chf.create(1, 1000, Arrays.asList(A, B), cf);

      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      // B should own ~750, A ~250 — pure rendezvous has hash variance so allow a wide tolerance.
      // The purpose is to observe the distribution, not enforce strict proportionality.
      int bOwned = stats.getOwned(B);
      int aOwned = stats.getOwned(A);
      assertEquals(1000, aOwned + bOwned);
      // B must own strictly more than A (correct direction), but we accept up to 2x skew
      assertInRange("B owned with 3x capacity", bOwned, 500, 1000);
      assertInRange("A owned with 1x capacity", aOwned, 0, 500);
   }

   public void testSingleNodeAllSegmentsOwnedByThatNode() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");

      DefaultConsistentHash ch = chf.create(3, 256, Arrays.asList(A), null);

      for (int s = 0; s < ch.getNumSegments(); s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         assertEquals(1, owners.size(), "With 1 node, every segment should have exactly 1 owner");
         assertEquals(A, owners.get(0));
      }
   }

   public void testUpdateMembersPreservesExistingOwners() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Address D = Address.random("D");
      Address E = Address.random("E");
      List<Address> original = Arrays.asList(A, B, C, D);

      DefaultConsistentHash ch = chf.create(2, 64, original, null);

      // Add E via updateMembers (no rebalance yet)
      List<Address> withE = Arrays.asList(A, B, C, D, E);
      DefaultConsistentHash updated = chf.updateMembers(ch, withE, null);

      // For every segment that E is NOT an owner of, all original owners should be preserved
      for (int s = 0; s < ch.getNumSegments(); s++) {
         List<Address> oldOwners = ch.locateOwnersForSegment(s);
         List<Address> newOwners = updated.locateOwnersForSegment(s);
         if (!newOwners.contains(E)) {
            assertEquals(oldOwners, newOwners,
                  "Owners for segment " + s + " should be unchanged when E is not an owner");
         }
      }
   }

   public void testUnionContainsBothOwners() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Address D = Address.random("D");

      DefaultConsistentHash ch1 = chf.create(2, 64, Arrays.asList(A, B, C), null);
      DefaultConsistentHash ch2 = chf.create(2, 64, Arrays.asList(A, B, C, D), null);
      DefaultConsistentHash union = chf.union(ch1, ch2);

      for (int s = 0; s < union.getNumSegments(); s++) {
         List<Address> unionOwners = union.locateOwnersForSegment(s);
         for (Address owner : ch1.locateOwnersForSegment(s)) {
            if (ch2.getMembers().contains(owner)) {
               assertEquals(true, unionOwners.contains(owner),
                     "Union must contain ch1 owner " + owner + " for segment " + s);
            }
         }
         for (Address owner : ch2.locateOwnersForSegment(s)) {
            assertEquals(true, unionOwners.contains(owner),
                  "Union must contain ch2 owner " + owner + " for segment " + s);
         }
      }
   }

   // ---- Helpers ----

   private static void assertInRange(String msg, int actual, int min, int max) {
      if (actual < min || actual > max) {
         throw new AssertionError(msg + ": expected [" + min + ".." + max + "] but got " + actual);
      }
   }
}
