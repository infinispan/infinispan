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
 * Tests capacity factor proportionality and edge cases for {@link HistoryHintedRendezvousConsistentHashFactory}.
 *
 * <p>Two complementary styles are used:</p>
 * <ul>
 *   <li><b>Fixed-address tests</b> — deterministic checks on single CH instances with a large
 *       segment count, verifying total ownership proportions tightly (±1–3%).</li>
 *   <li><b>Statistical tests</b> — run many rounds with independent random addresses and
 *       accumulate primary counts, verifying that the average converges to the expected proportion
 *       within 5%. This catches variance that a single random seed might mask.</li>
 * </ul>
 */
@Test(groups = "unit", testName = "distribution.ch.RendezvousCapacityFactorTest")
public class RendezvousCapacityFactorTest extends AbstractInfinispanTest {

   private static final ConsistentHashFactory<DefaultConsistentHash> CHF =
         HistoryHintedRendezvousConsistentHashFactory.getInstance();

   private static final int ROUNDS = 500;

   // =========================================================================
   // Fixed-address: total ownership proportionality
   // =========================================================================

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

   /**
    * With {@code numOwners=2} total ownership (primary + backup) should still be proportional to
    * each node's capacity factor. A larger tolerance is necessary because the balancer spreads
    * copies across both positions rather than placing every copy independently.
    */
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

   // =========================================================================
   // Fixed-address: edge cases
   // =========================================================================

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

   // =========================================================================
   // Statistical: numOwners=1 (pure case) — primary ownership convergence
   //
   // Runs ROUNDS independent random address sets so that the average primary
   // count converges to the expected proportion, catching hash-variance issues
   // that a single seed might hide.
   // =========================================================================

   /**
    * With {@code numOwners=1} every segment has exactly one owner. Verifies the structural
    * invariant across {@value #ROUNDS} independent random address pairs.
    */
   public void testPureCaseExactlyOneOwnerPerSegment() {
      int numSegments = 256;
      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         List<Address> members = Arrays.asList(a, b);
         Map<Address, Float> cfs = new HashMap<>();
         cfs.put(a, 1f);
         cfs.put(b, 2f);

         DefaultConsistentHash ch = CHF.create(1, numSegments, members, cfs);

         for (int s = 0; s < numSegments; s++) {
            List<Address> owners = ch.locateOwnersForSegment(s);
            assertEquals(1, owners.size(),
                  "Round " + i + " segment " + s + ": expected exactly 1 owner, got " + owners.size());
         }
      }
   }

   /**
    * Uniform capacity ({@code null} map), 2 nodes, {@code numOwners=1}: each node should average
    * ~50% primary ownership across {@value #ROUNDS} random address pairs (±5%).
    */
   public void testStatisticalPrimaryUniformTwoNodes() {
      int numSegments = 256;
      long totalA = 0, totalB = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         List<Address> members = Arrays.asList(a, b);

         DefaultConsistentHash ch = CHF.create(1, numSegments, members, null);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
      }

      assertNear("uniform 2-node: avg primary A", (double) totalA / ROUNDS,
            numSegments / 2.0, numSegments * 0.05);
      assertNear("uniform 2-node: avg primary B", (double) totalB / ROUNDS,
            numSegments / 2.0, numSegments * 0.05);
      assertEquals((long) numSegments * ROUNDS, totalA + totalB,
            "Total primary must equal numSegments × rounds");
   }

   /**
    * Capacity 1:2, 2 nodes, {@code numOwners=1}: A averages ~1/3, B ~2/3 primary ownership
    * across {@value #ROUNDS} random pairs (±5%).
    */
   public void testStatisticalPrimary1to2TwoNodes() {
      int numSegments = 256;
      long totalA = 0, totalB = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         List<Address> members = Arrays.asList(a, b);
         Map<Address, Float> cfs = new HashMap<>();
         cfs.put(a, 1f);
         cfs.put(b, 2f);

         DefaultConsistentHash ch = CHF.create(1, numSegments, members, cfs);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
      }

      assertNear("1:2 2-node: avg primary A (1/3)", (double) totalA / ROUNDS,
            numSegments / 3.0, numSegments * 0.05);
      assertNear("1:2 2-node: avg primary B (2/3)", (double) totalB / ROUNDS,
            numSegments * 2.0 / 3.0, numSegments * 0.05);
      assertEquals((long) numSegments * ROUNDS, totalA + totalB,
            "Total primary must equal numSegments × rounds");
   }

   /**
    * Capacity 1:3, 2 nodes, {@code numOwners=1}: A averages ~25%, B ~75% (±5%).
    */
   public void testStatisticalPrimary1to3TwoNodes() {
      int numSegments = 256;
      long totalA = 0, totalB = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         List<Address> members = Arrays.asList(a, b);
         Map<Address, Float> cfs = new HashMap<>();
         cfs.put(a, 1f);
         cfs.put(b, 3f);

         DefaultConsistentHash ch = CHF.create(1, numSegments, members, cfs);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
      }

      assertNear("1:3 2-node: avg primary A (25%)", (double) totalA / ROUNDS,
            numSegments / 4.0, numSegments * 0.05);
      assertNear("1:3 2-node: avg primary B (75%)", (double) totalB / ROUNDS,
            numSegments * 3.0 / 4.0, numSegments * 0.05);
   }

   // =========================================================================
   // Statistical: numOwners=2 — primary ownership with backup replication
   //
   // With numOwners=2 each segment has a primary and one backup. The balancer
   // must still distribute primary ownership proportionally to capacity factor
   // even though it is now a secondary concern after total-ownership balance.
   // =========================================================================

   /**
    * Capacity 1:2, 3 nodes, {@code numOwners=2}: primary ownership should converge to the
    * capacity-proportional share averaged across {@value #ROUNDS} random triplets (±8%).
    *
    * <p>The tolerance is looser than the {@code numOwners=1} case because the primary-redistribution
    * pass operates within the constraint set by total-ownership balance — it can't always reach the
    * exact ideal when that ideal conflicts with an optimal total assignment.</p>
    */
   public void testStatisticalPrimaryNumOwners2ThreeNodes() {
      int numSegments = 256;
      // 3 nodes, capacity 1:2:3, numOwners=2
      // Primary ideal per node: same proportional split as numOwners=1
      // A=1/6 ≈ 42.7, B=2/6 ≈ 85.3, C=3/6 = 128
      long totalA = 0, totalB = 0, totalC = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         Address c = Address.random();
         List<Address> members = Arrays.asList(a, b, c);
         Map<Address, Float> cfs = new HashMap<>();
         cfs.put(a, 1f);
         cfs.put(b, 2f);
         cfs.put(c, 3f);

         DefaultConsistentHash ch = CHF.create(2, numSegments, members, cfs);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
         totalC += stats.getPrimaryOwned(c);

         // Structural: every segment must have exactly 2 owners
         for (int s = 0; s < numSegments; s++) {
            assertEquals(2, ch.locateOwnersForSegment(s).size(),
                  "Round " + i + " segment " + s + ": expected 2 owners");
         }
      }

      double tolerance = numSegments * 0.08; // 8% — looser for numOwners=2
      assertNear("1:2:3 3-node numOwners=2: avg primary A (1/6)", (double) totalA / ROUNDS,
            numSegments / 6.0, tolerance);
      assertNear("1:2:3 3-node numOwners=2: avg primary B (2/6)", (double) totalB / ROUNDS,
            numSegments * 2.0 / 6.0, tolerance);
      assertNear("1:2:3 3-node numOwners=2: avg primary C (3/6)", (double) totalC / ROUNDS,
            numSegments * 3.0 / 6.0, tolerance);
      assertEquals((long) numSegments * ROUNDS, totalA + totalB + totalC,
            "Total primary must equal numSegments × rounds");
   }

   /**
    * Uniform capacity, 4 nodes, {@code numOwners=2}: each node should average ~25% primary
    * ownership across {@value #ROUNDS} random quadruplets (±5%).
    */
   public void testStatisticalPrimaryNumOwners2FourNodesUniform() {
      int numSegments = 256;
      long totalA = 0, totalB = 0, totalC = 0, totalD = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         Address c = Address.random();
         Address d = Address.random();
         List<Address> members = Arrays.asList(a, b, c, d);

         DefaultConsistentHash ch = CHF.create(2, numSegments, members, null);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
         totalC += stats.getPrimaryOwned(c);
         totalD += stats.getPrimaryOwned(d);

         for (int s = 0; s < numSegments; s++) {
            assertEquals(2, ch.locateOwnersForSegment(s).size(),
                  "Round " + i + " segment " + s + ": expected 2 owners");
         }
      }

      double expected = numSegments / 4.0;
      double tolerance = numSegments * 0.05;
      assertNear("uniform 4-node numOwners=2: avg primary A", (double) totalA / ROUNDS, expected, tolerance);
      assertNear("uniform 4-node numOwners=2: avg primary B", (double) totalB / ROUNDS, expected, tolerance);
      assertNear("uniform 4-node numOwners=2: avg primary C", (double) totalC / ROUNDS, expected, tolerance);
      assertNear("uniform 4-node numOwners=2: avg primary D", (double) totalD / ROUNDS, expected, tolerance);
      assertEquals((long) numSegments * ROUNDS, totalA + totalB + totalC + totalD,
            "Total primary must equal numSegments × rounds");
   }

   // =========================================================================
   // Statistical: numOwners=3 — primary ownership with two backup copies
   // =========================================================================

   /**
    * Capacity 1:2:3, 4 nodes, {@code numOwners=3}: primary ownership should converge to the
    * capacity-proportional share across {@value #ROUNDS} random quadruplets (±8%).
    *
    * <p>Each segment has 3 owners. Total capacity = 1+2+3+4 = 10.
    * Primary ideal: A=10%, B=20%, C=30%, D=40% of 256 segments.</p>
    */
   public void testStatisticalPrimaryNumOwners3FourNodes() {
      int numSegments = 256;
      long totalA = 0, totalB = 0, totalC = 0, totalD = 0;

      for (int i = 0; i < ROUNDS; i++) {
         Address a = Address.random();
         Address b = Address.random();
         Address c = Address.random();
         Address d = Address.random();
         List<Address> members = Arrays.asList(a, b, c, d);
         Map<Address, Float> cfs = new HashMap<>();
         cfs.put(a, 1f);
         cfs.put(b, 2f);
         cfs.put(c, 3f);
         cfs.put(d, 4f);

         DefaultConsistentHash ch = CHF.create(3, numSegments, members, cfs);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         totalA += stats.getPrimaryOwned(a);
         totalB += stats.getPrimaryOwned(b);
         totalC += stats.getPrimaryOwned(c);
         totalD += stats.getPrimaryOwned(d);

         for (int s = 0; s < numSegments; s++) {
            assertEquals(3, ch.locateOwnersForSegment(s).size(),
                  "Round " + i + " segment " + s + ": expected 3 owners");
         }
      }

      double tolerance = numSegments * 0.08;
      assertNear("1:2:3:4 4-node numOwners=3: avg primary A (10%)", (double) totalA / ROUNDS,
            numSegments * 0.10, tolerance);
      assertNear("1:2:3:4 4-node numOwners=3: avg primary B (20%)", (double) totalB / ROUNDS,
            numSegments * 0.20, tolerance);
      assertNear("1:2:3:4 4-node numOwners=3: avg primary C (30%)", (double) totalC / ROUNDS,
            numSegments * 0.30, tolerance);
      assertNear("1:2:3:4 4-node numOwners=3: avg primary D (40%)", (double) totalD / ROUNDS,
            numSegments * 0.40, tolerance);
      assertEquals((long) numSegments * ROUNDS, totalA + totalB + totalC + totalD,
            "Total primary must equal numSegments × rounds");
   }

   /**
    * Uniform capacity, 5 nodes, {@code numOwners=3}: each node should average ~20% primary
    * ownership across {@value #ROUNDS} random sets (±5%).
    */
   public void testStatisticalPrimaryNumOwners3FiveNodesUniform() {
      int numSegments = 256;
      long[] totals = new long[5];

      for (int i = 0; i < ROUNDS; i++) {
         Address[] nodes = new Address[5];
         for (int j = 0; j < 5; j++) nodes[j] = Address.random();
         List<Address> members = Arrays.asList(nodes);

         DefaultConsistentHash ch = CHF.create(3, numSegments, members, null);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         for (int j = 0; j < 5; j++) totals[j] += stats.getPrimaryOwned(nodes[j]);

         for (int s = 0; s < numSegments; s++) {
            assertEquals(3, ch.locateOwnersForSegment(s).size(),
                  "Round " + i + " segment " + s + ": expected 3 owners");
         }
      }

      double expected = numSegments / 5.0;
      double tolerance = numSegments * 0.05;
      for (int j = 0; j < 5; j++) {
         assertNear("uniform 5-node numOwners=3: avg primary node[" + j + "]",
               (double) totals[j] / ROUNDS, expected, tolerance);
      }
      long grandTotal = 0;
      for (long t : totals) grandTotal += t;
      assertEquals((long) numSegments * ROUNDS, grandTotal,
            "Total primary must equal numSegments × rounds");
   }

   // =========================================================================
   // Helpers
   // =========================================================================

   private static void assertInRange(String msg, int actual, int min, int max) {
      if (actual < min || actual > max) {
         throw new AssertionError(msg + ": expected [" + min + ".." + max + "] but got " + actual);
      }
   }

   private static void assertNear(String msg, double actual, double expected, double tolerance) {
      if (Math.abs(actual - expected) > tolerance) {
         throw new AssertionError(msg + ": expected " + expected + " ± " + tolerance
               + " but got " + actual);
      }
   }
}
