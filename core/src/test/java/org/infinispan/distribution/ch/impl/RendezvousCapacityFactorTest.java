package org.infinispan.distribution.ch.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tests capacity factor proportionality and edge cases for {@link RendezvousConsistentHashFactory}.
 */
@Test(groups = "unit", testName = "distribution.ch.RendezvousCapacityFactorTest")
public class RendezvousCapacityFactorTest extends AbstractInfinispanTest {

   private static final ConsistentHashFactory<DefaultConsistentHash> CHF =
         RendezvousConsistentHashFactory.getInstance();

   public void testProportionalDistributionTwoNodes() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 3f);

      DefaultConsistentHash ch = CHF.create(1, 256, Arrays.asList(A, B), cf);
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());

      // A should own ~64 (25%), B ~192 (75%) — allow ±3
      assertInRange("A (1x)", stats.getOwned(A), 61, 67);
      assertInRange("B (3x)", stats.getOwned(B), 189, 195);
   }

   public void testProportionalDistributionFourNodes() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Address D = Address.random("D");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 2f);
      cf.put(C, 3f);
      cf.put(D, 4f);

      DefaultConsistentHash ch = CHF.create(1, 1000, Arrays.asList(A, B, C, D), cf);
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      // Total = 10, so A=10%, B=20%, C=30%, D=40%
      assertInRange("A (10%)", stats.getOwned(A), 97, 103);
      assertInRange("B (20%)", stats.getOwned(B), 197, 203);
      assertInRange("C (30%)", stats.getOwned(C), 297, 303);
      assertInRange("D (40%)", stats.getOwned(D), 397, 403);
   }

   public void testProportionalDistributionWithNumOwners() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 2f);
      cf.put(C, 3f);

      // numOwners=2, 300 segments, totalCopies=600. Proportions: A=100, B=200, C=300
      DefaultConsistentHash ch = CHF.create(2, 300, Arrays.asList(A, B, C), cf);
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      // Allow ±15% — the greedy single-pass can drift by a few percent with numOwners>1
      assertInRange("A (1x, numOwners=2)", stats.getOwned(A), 85, 115);
      assertInRange("B (2x, numOwners=2)", stats.getOwned(B), 170, 230);
      assertInRange("C (3x, numOwners=2)", stats.getOwned(C), 255, 345);
   }

   public void testZeroCapacityNodeExcluded() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 0f);
      cf.put(C, 1f);
      List<Address> members = Arrays.asList(A, B, C);

      DefaultConsistentHash ch = CHF.create(2, 100, members, cf);

      for (int s = 0; s < ch.getNumSegments(); s++) {
         assertFalse(ch.locateOwnersForSegment(s).contains(B),
               "Zero-capacity node B must not own segment " + s);
      }
      assertEquals(members, ch.getMembers(), "B must still be in the member list");
   }

   public void testAllZeroCapacityThrows() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 0f);
      cf.put(B, 0f);

      assertThrows(IllegalArgumentException.class,
            () -> CHF.create(2, 64, Arrays.asList(A, B), cf),
            "All-zero capacity should throw IllegalArgumentException");
   }

   public void testNullMapEqualsAllOnes() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Map<Address, Float> allOnes = new HashMap<>();
      allOnes.put(A, 1f);
      allOnes.put(B, 1f);

      DefaultConsistentHash chNull = CHF.create(2, 64, Arrays.asList(A, B), null);
      DefaultConsistentHash chOnes = CHF.create(2, 64, Arrays.asList(A, B), allOnes);
      assertEquals(chNull, chOnes, "null capacity map and all-1.0 map must produce identical CHs");
   }

   public void testCapacityFactorScalesConsistentlyWithNumOwners() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 3f);
      List<Address> members = Arrays.asList(A, B);

      for (int numOwners : new int[]{1, 2, 3}) {
         DefaultConsistentHash ch = CHF.create(numOwners, 256, members, cf);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         int aOwned = stats.getOwned(A);
         int bOwned = stats.getOwned(B);
         // When numOwners >= numNodes (2 nodes), every segment has both owners so ratio = 1 — skip
         if (numOwners >= members.size()) continue;
         double ratio = (double) bOwned / aOwned;
         // B should own ~3x A — allow 20% slack for the greedy single-pass
         assertInRange("B/A ratio for numOwners=" + numOwners,
               (int) (ratio * 100), 240, 360);
      }
   }

   public void testSingleHighCapacityNode() {
      Address high = Address.random("high");
      List<Address> members = new java.util.ArrayList<>();
      members.add(high);
      Map<Address, Float> cf = new HashMap<>();
      cf.put(high, 100f);
      for (int i = 0; i < 10; i++) {
         Address node = Address.random("n" + i);
         members.add(node);
         cf.put(node, 1f);
      }

      DefaultConsistentHash ch = CHF.create(1, 512, members, cf);
      OwnershipStatistics stats = new OwnershipStatistics(ch, members);
      // high has 100/(100+10) ≈ 90.9% → ~465 of 512
      assertInRange("high-capacity node (100x)", stats.getOwned(high), 460, 470);
   }

   // ---- Helpers ----

   private static void assertInRange(String msg, int actual, int min, int max) {
      if (actual < min || actual > max) {
         throw new AssertionError(msg + ": expected [" + min + ".." + max + "] but got " + actual);
      }
   }
}
