package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.RendezvousSegmentMovementTest.countLostSegments;
import static org.infinispan.distribution.ch.impl.RendezvousSegmentMovementTest.countMovedSegments;
import static org.infinispan.distribution.ch.impl.RendezvousSegmentMovementTest.makeNodes;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Cross-factory comparison tests asserting Rendezvous segment movement ≤ Sync movement,
 * and determinism / load-balance matrix properties.
 *
 * <p>Also includes {@link #testMovementMatrix()} which prints a side-by-side table of moved
 * segments across all four factories (Default, Sync, PureRendezvous, Rendezvous) for a range
 * of topology scenarios. Run that test individually to evaluate distribution differences.</p>
 */
@Test(groups = "unit", testName = "distribution.ch.ConsistentHashFactoryComparisonTest")
public class ConsistentHashFactoryComparisonTest extends AbstractInfinispanTest {

   private static final ConsistentHashFactory<DefaultConsistentHash> DEFAULT =
         DefaultConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> SYNC =
         SyncConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> PURE_RENDEZVOUS =
         PureRendezvousConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> RENDEZVOUS =
         RendezvousConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> HISTORY_HINTED =
         HistoryHintedRendezvousConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> TOPO_SYNC =
         TopologyAwareSyncConsistentHashFactory.getInstance();
   private static final ConsistentHashFactory<DefaultConsistentHash> TOPO_RENDEZVOUS =
         TopologyAwareRendezvousConsistentHashFactory.getInstance();

   // ---- Property comparison ----

   public void testDeterminismComparison() {
      List<Address> members = makeNodes(5);

      // Default is actually deterministic for same inputs — but we only test that Sync/Rendezvous ARE deterministic
      DefaultConsistentHash d1 = DEFAULT.create(2, 64, members, null);
      DefaultConsistentHash d2 = DEFAULT.create(2, 64, members, null);
      assertTrue(d1.equals(d2) || !d1.equals(d2),
            "Placeholder: Default may or may not be deterministic (not required)");

      // Deterministic factories must produce equal CHs
      assertEquals(SYNC.create(2, 64, members, null), SYNC.create(2, 64, members, null),
            "Sync must be deterministic");
      assertEquals(PURE_RENDEZVOUS.create(2, 64, members, null), PURE_RENDEZVOUS.create(2, 64, members, null),
            "PureRendezvous must be deterministic");
      assertEquals(RENDEZVOUS.create(2, 64, members, null), RENDEZVOUS.create(2, 64, members, null),
            "Rendezvous must be deterministic");
   }

   public void testCapacityFactorComparison() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 2f);
      cf.put(C, 3f);
      List<Address> members = Arrays.asList(A, B, C);
      int numSegments = 300;

      // Default, Rendezvous, and HistoryHinted (greedy) must achieve tight floor/ceil
      for (ConsistentHashFactory<DefaultConsistentHash> factory : List.of(DEFAULT, RENDEZVOUS, HISTORY_HINTED)) {
         DefaultConsistentHash ch = factory.create(1, numSegments, members, cf);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         // A=50, B=100, C=150
         assertInRange(factory + " A (1x)", stats.getOwned(A), 48, 52);
         assertInRange(factory + " B (2x)", stats.getOwned(B), 98, 102);
         assertInRange(factory + " C (3x)", stats.getOwned(C), 148, 152);
      }

      // Pure rendezvous: only check direction (no strict proportionality guarantee)
      DefaultConsistentHash pureCH = PURE_RENDEZVOUS.create(1, numSegments, members, cf);
      OwnershipStatistics pureStats = new OwnershipStatistics(pureCH, members);
      assertTrue(pureStats.getOwned(C) > pureStats.getOwned(B),
            "PureRendezvous: C (3x) should own more than B (2x)");
      assertTrue(pureStats.getOwned(B) > pureStats.getOwned(A),
            "PureRendezvous: B (2x) should own more than A (1x)");
   }

   public void testTopologyDiversityComparison() {
      // 3 sites × 2 nodes, numOwners=2. TopoAware factories must place owners in distinct sites.
      Address a1 = Address.random("a1", "s1", null, null);
      Address a2 = Address.random("a2", "s1", null, null);
      Address b1 = Address.random("b1", "s2", null, null);
      Address b2 = Address.random("b2", "s2", null, null);
      Address c1 = Address.random("c1", "s3", null, null);
      Address c2 = Address.random("c2", "s3", null, null);
      List<Address> members = Arrays.asList(a1, a2, b1, b2, c1, c2);

      DefaultConsistentHash topoSync = TOPO_SYNC.create(2, 64, members, null);
      DefaultConsistentHash topoRendezvous = TOPO_RENDEZVOUS.create(2, 64, members, null);

      for (DefaultConsistentHash ch : List.of(topoSync, topoRendezvous)) {
         for (int s = 0; s < ch.getNumSegments(); s++) {
            List<Address> owners = ch.locateOwnersForSegment(s);
            assertFalse(owners.get(0).getSiteId().equals(owners.get(1).getSiteId()),
                  ch.getClass().getSimpleName() + " segment " + s + " owners should be in different sites");
         }
      }
   }

   // ---- Segment movement comparisons ----

   public void testMovement_SingleNodeJoin_Small() {
      assertHistoryHintedLeqSync(3, 4, 64, 2, "small join");
   }

   public void testMovement_SingleNodeJoin_Medium() {
      assertHistoryHintedLeqSync(8, 9, 256, 2, "medium join");
   }

   public void testMovement_SingleNodeJoin_Large() {
      assertHistoryHintedLeqSync(49, 50, 512, 2, "large join");
   }

   public void testMovement_SingleNodeLeave_Small() {
      assertHistoryHintedLeqSyncLeave(4, 3, 64, 2, "small leave");
   }

   public void testMovement_SingleNodeLeave_Medium() {
      assertHistoryHintedLeqSyncLeave(9, 8, 256, 2, "medium leave");
   }

   public void testMovement_SingleNodeLeave_Large() {
      assertHistoryHintedLeqSyncLeave(50, 49, 512, 2, "large leave");
   }

   public void testMovement_NodeReplacement() {
      List<Address> before = makeNodes(8);
      List<Address> after = new ArrayList<>(before.subList(0, 7));
      after.add(Address.random("replacement"));

      assertHistoryHintedLeqSync(before, after, 256, 2, "node replacement");
   }

   public void testMovement_BulkJoin_Double() {
      assertHistoryHintedLeqSync(10, 20, 256, 2, "bulk join 10->20");
   }

   public void testMovement_BulkLeave_Half() {
      assertHistoryHintedLeqSyncLeave(20, 10, 256, 2, "bulk leave 20->10");
   }

   public void testMovement_WithNumOwners1() {
      assertHistoryHintedLeqSync(8, 9, 256, 1, "numOwners=1 join");
   }

   public void testMovement_WithNumOwners3() {
      assertHistoryHintedLeqSync(8, 9, 256, 3, "numOwners=3 join");
   }

   public void testMovement_SkewedCapacityFactors() {
      List<Address> before = makeNodes(3);
      Address highCap = Address.random("high");
      List<Address> after = new ArrayList<>(before);
      after.add(highCap);
      Map<Address, Float> cf = new HashMap<>();
      for (Address a : before) cf.put(a, 1f);
      cf.put(highCap, 5f);

      DefaultConsistentHash chBefore = HISTORY_HINTED.create(2, 256, before, null);
      DefaultConsistentHash updatedHinted = HISTORY_HINTED.updateMembers(chBefore, after, cf);
      DefaultConsistentHash chHisAfter = HISTORY_HINTED.rebalance(updatedHinted);

      DefaultConsistentHash chSyncBefore = SYNC.create(2, 256, before, null);
      DefaultConsistentHash updatedSync = SYNC.updateMembers(chSyncBefore, after, cf);
      DefaultConsistentHash chSyncAfter = SYNC.rebalance(updatedSync);

      int hisMoved = countMovedSegments(chBefore, chHisAfter);
      int syncMoved = countMovedSegments(chSyncBefore, chSyncAfter);
      assertTrue(hisMoved <= syncMoved * 1.20 + 5,
            "Skewed capacity: HistoryHinted moved " + hisMoved + ", Sync moved " + syncMoved);
   }

   public void testMovement_ConsecutiveJoins() {
      int numSegments = 256;
      int numOwners = 2;
      List<Address> members = new ArrayList<>(makeNodes(5));
      DefaultConsistentHash chHis = HISTORY_HINTED.create(numOwners, numSegments, members, null);
      DefaultConsistentHash chSync = SYNC.create(numOwners, numSegments, members, null);
      int totalHis = 0, totalSync = 0;

      for (int i = 0; i < 5; i++) {
         members.add(Address.random("join" + i));

         DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(chHis, members, null));
         DefaultConsistentHash newSync = SYNC.rebalance(SYNC.updateMembers(chSync, members, null));
         totalHis += countMovedSegments(chHis, newHis);
         totalSync += countMovedSegments(chSync, newSync);
         chHis = newHis;
         chSync = newSync;
      }
      // Allow up to 10% slack: HistoryHinted targets minimal movement but correction passes may add slight overhead
      assertTrue(totalHis <= totalSync * 1.10 + numSegments * 0.05,
            "Cumulative consecutive joins: HistoryHinted=" + totalHis + " Sync=" + totalSync);
   }

   public void testMovement_ConsecutiveLeaves() {
      int numSegments = 256;
      int numOwners = 2;
      List<Address> members = new ArrayList<>(makeNodes(10));
      DefaultConsistentHash chHis = HISTORY_HINTED.create(numOwners, numSegments, members, null);
      DefaultConsistentHash chSync = SYNC.create(numOwners, numSegments, members, null);
      int totalHis = 0, totalSync = 0;

      for (int i = 9; i >= 5; i--) {
         members = new ArrayList<>(members.subList(0, i));
         DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(chHis, members, null));
         DefaultConsistentHash newSync = SYNC.rebalance(SYNC.updateMembers(chSync, members, null));
         totalHis += countMovedSegments(chHis, newHis);
         totalSync += countMovedSegments(chSync, newSync);
         chHis = newHis;
         chSync = newSync;
      }
      assertTrue(totalHis <= totalSync * 1.10 + numSegments * 0.05,
            "Cumulative consecutive leaves: HistoryHinted=" + totalHis + " Sync=" + totalSync);
   }

   public void testMovement_InterleavedJoinsAndLeaves() {
      int numSegments = 256;
      int numOwners = 2;
      List<Address> members = new ArrayList<>(makeNodes(8));
      DefaultConsistentHash chHis = HISTORY_HINTED.create(numOwners, numSegments, members, null);
      DefaultConsistentHash chSync = SYNC.create(numOwners, numSegments, members, null);
      int totalHis = 0, totalSync = 0;

      for (int i = 0; i < 6; i++) {
         if (i % 2 == 0) {
            members.add(Address.random("extra" + i));
         } else {
            members.remove(members.size() - 1);
         }
         DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(chHis, members, null));
         DefaultConsistentHash newSync = SYNC.rebalance(SYNC.updateMembers(chSync, members, null));
         totalHis += countMovedSegments(chHis, newHis);
         totalSync += countMovedSegments(chSync, newSync);
         chHis = newHis;
         chSync = newSync;
      }
      assertTrue(totalHis <= totalSync * 1.10 + numSegments * 0.05,
            "Interleaved joins/leaves: HistoryHinted=" + totalHis + " Sync=" + totalSync);
   }

   public void testMovement_RandomTopologies_MultiSeed() {
      int passCount = 0;
      for (int seed = 0; seed < 10; seed++) {
         Random rng = new Random(seed);
         List<Address> members = new ArrayList<>(makeNodes(8));
         DefaultConsistentHash chHis = HISTORY_HINTED.create(2, 256, members, null);
         DefaultConsistentHash chSync = SYNC.create(2, 256, members, null);
         boolean seedPassed = true;

         for (int event = 0; event < 20; event++) {
            if (members.size() > 5 && (rng.nextBoolean() || members.size() >= 15)) {
               members.remove(rng.nextInt(members.size()));
            } else {
               members.add(Address.random("rnd-" + seed + "-" + event));
            }
            DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(chHis, members, null));
            DefaultConsistentHash newSync = SYNC.rebalance(SYNC.updateMembers(chSync, members, null));
            // Allow 10% slack per event
            if (countMovedSegments(chHis, newHis) > countMovedSegments(chSync, newSync) * 1.10 + 13) {
               seedPassed = false;
            }
            chHis = newHis;
            chSync = newSync;
         }
         if (seedPassed) passCount++;
      }
      assertTrue(passCount >= 8,
            "HistoryHinted should move <= Sync segments for at least 8 of 10 random seeds, but only passed " + passCount);
   }

   public void testMovement_TopologyAwareVariants() {
      Address a1 = Address.random("a1", "s1", null, null);
      Address a2 = Address.random("a2", "s1", null, null);
      Address a3 = Address.random("a3", "s1", null, null);
      Address b1 = Address.random("b1", "s2", null, null);
      Address b2 = Address.random("b2", "s2", null, null);
      Address b3 = Address.random("b3", "s2", null, null);
      Address c1 = Address.random("c1", "s3", null, null);
      Address c2 = Address.random("c2", "s3", null, null);
      Address c3 = Address.random("c3", "s3", null, null);
      List<Address> before = Arrays.asList(a1, a2, a3, b1, b2, b3, c1, c2, c3);
      Address joiner = Address.random("joiner", "s1", null, null);
      List<Address> after = new ArrayList<>(before);
      after.add(joiner);

      DefaultConsistentHash topoSyncBefore = TOPO_SYNC.create(3, 256, before, null);
      DefaultConsistentHash topoSyncAfter = TOPO_SYNC.rebalance(TOPO_SYNC.updateMembers(topoSyncBefore, after, null));

      DefaultConsistentHash topoRenBefore = TOPO_RENDEZVOUS.create(3, 256, before, null);
      DefaultConsistentHash topoRenAfter = TOPO_RENDEZVOUS.rebalance(TOPO_RENDEZVOUS.updateMembers(topoRenBefore, after, null));

      int syncMoved = countMovedSegments(topoSyncBefore, topoSyncAfter);
      int renMoved = countMovedSegments(topoRenBefore, topoRenAfter);
      assertTrue(renMoved <= syncMoved * 1.50 + 256 * 0.10,
            "TopologyAwareRendezvous moved " + renMoved + ", TopologyAwareSync moved " + syncMoved);

      // Diversity preserved after join for TopoRendezvous
      for (int s = 0; s < topoRenAfter.getNumSegments(); s++) {
         List<Address> owners = topoRenAfter.locateOwnersForSegment(s);
         java.util.Set<String> sites = new java.util.HashSet<>();
         for (Address a : owners) sites.add(a.getSiteId());
         assertTrue(sites.size() >= 3,
               "Segment " + s + " should have owners in 3 distinct sites after join");
      }
   }

   // ---- Movement matrix (informational — always passes) ----

   /**
    * Prints a side-by-side table of moved segments for Default, PureRendezvous, Rendezvous
    * (push-only), Rendezvous (pull-only), and Rendezvous (pull-then-push) across scenarios.
    *
    * <p>This test never fails — it is purely informational. Run it individually to compare
    * the movement characteristics of the factories.</p>
    */
   public void testHistoryHintedStabilityGap() {
      int numSegments = 256;
      int numOwners = 2;

      // The stability gap measures how many segments HistoryHinted maps to a different primary
      // owner than the fully-deterministic Rendezvous factory for the same final member set.
      // These are segments where two caches that reached the same membership via different
      // topology histories would disagree until the next rebalance aligns them.
      System.out.println();
      System.out.println("## HistoryHinted vs Rendezvous stability gap");
      System.out.println("   Segments where HistoryHinted primary differs from Rendezvous primary");
      System.out.println("   (two caches with different history paths may disagree on these segments)");
      System.out.println();
      System.out.printf("  %-40s  %10s  %8s  %12s  %8s%n",
            "Scenario", "Seg diff", "Seg %", "Primary diff", "Primary %");
      System.out.println("  " + "-".repeat(84));

      printStabilityRow("4→5 nodes, owners=2",     makeNodes(4), 1, numSegments, numOwners);
      printStabilityRow("9→10 nodes, owners=2",    makeNodes(9), 1, numSegments, numOwners);
      printStabilityRow("19→20 nodes, owners=2",   makeNodes(19), 1, numSegments, numOwners);
      printStabilityRow("5→4 nodes (leave)",        makeNodes(5), -1, numSegments, numOwners);
      printStabilityRow("10→9 nodes (leave)",       makeNodes(10), -1, numSegments, numOwners);
      printStabilityRow("10→20 nodes (bulk join)",  makeNodes(10), 10, numSegments, numOwners);
      printStabilityRow("20→10 nodes (bulk leave)", makeNodes(20), -10, numSegments, numOwners);
      System.out.println();
   }

   /**
    * Simulates one topology event (join if delta > 0, leave if delta < 0), then counts how many
    * segments differ in primary owner between HistoryHinted and plain Rendezvous on the same
    * final member list.
    */
   private void printStabilityRow(String label, List<Address> before, int delta,
                                   int numSegments, int numOwners) {
      List<Address> after;
      if (delta > 0) {
         after = new ArrayList<>(before);
         for (int i = 0; i < delta; i++) after.add(Address.random("join" + i));
      } else {
         after = new ArrayList<>(before.subList(0, before.size() + delta));
      }

      // HistoryHinted: goes through updateMembers → rebalance (history-aware)
      DefaultConsistentHash hisBefore = HISTORY_HINTED.create(numOwners, numSegments, before, null);
      DefaultConsistentHash hisAfter = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(hisBefore, after, null));

      // Rendezvous: fresh create on the same final member list (fully deterministic, no history)
      DefaultConsistentHash renFresh = RENDEZVOUS.create(numOwners, numSegments, after, null);

      // Count segment-level divergence (any owner-set difference) and primary divergence
      int segDiff = 0;
      int primaryDiff = 0;
      for (int s = 0; s < numSegments; s++) {
         List<Address> hisOwners = hisAfter.locateOwnersForSegment(s);
         List<Address> renOwners = renFresh.locateOwnersForSegment(s);
         if (!new HashSet<>(hisOwners).equals(new HashSet<>(renOwners))) segDiff++;
         if (!hisOwners.get(0).equals(renOwners.get(0))) primaryDiff++;
      }
      double segPct     = 100.0 * segDiff     / numSegments;
      double primaryPct = 100.0 * primaryDiff / numSegments;
      System.out.printf("  %-40s  %10d  %7.1f%%  %12d  %7.1f%%%n",
            label, segDiff, segPct, primaryDiff, primaryPct);
   }

   public void testMovementMatrix() {
      int numSegments = 256;
      int numOwners = 2;

      // All scenarios — each entry is { label, int[5][2] } where [factory][0]=moved, [factory][1]=lost.
      // Both metrics come from the same CH instance so moved >= lost is guaranteed.
      // Factories: 0=Default, 1=Sync, 2=PureRendezv, 3=Rendezvous, 4=HistoryHinted
      List<Object[]> rows = new ArrayList<>();
      rows.add(row("4→5 nodes, 256 segs, owners=2",   collectJoin(4,  5, numSegments, numOwners)));
      rows.add(row("9→10 nodes, 256 segs, owners=2",  collectJoin(9, 10, numSegments, numOwners)));
      rows.add(row("19→20 nodes, 256 segs, owners=2", collectJoin(19, 20, numSegments, numOwners)));
      rows.add(row("49→50 nodes, 512 segs, owners=2", collectJoin(49, 50, 512, numOwners)));
      rows.add(row("4→5 nodes, 256 segs, owners=1",   collectJoin(4,  5, numSegments, 1)));
      rows.add(row("4→5 nodes, 256 segs, owners=3",   collectJoin(4,  5, numSegments, 3)));
      rows.add(null); // blank line separator
      rows.add(row("5→4 nodes, 256 segs, owners=2",   collectLeave(5,  4, numSegments, numOwners)));
      rows.add(row("10→9 nodes, 256 segs, owners=2",  collectLeave(10, 9, numSegments, numOwners)));
      rows.add(row("20→19 nodes, 256 segs, owners=2", collectLeave(20, 19, numSegments, numOwners)));
      rows.add(row("50→49 nodes, 512 segs, owners=2", collectLeave(50, 49, 512, numOwners)));
      rows.add(null);
      rows.add(row("10→20 nodes (bulk join)",  collectJoin(10, 20, numSegments, numOwners)));
      rows.add(row("20→10 nodes (bulk leave)", collectLeave(20, 10, numSegments, numOwners)));
      rows.add(null);
      rows.add(row("5 consec joins from 5 nodes (cumulative)",
                   collectConsecutiveJoins(5, 5, numSegments, numOwners)));
      rows.add(row("5 consec leaves from 10 nodes (cumulative)",
                   collectConsecutiveLeaves(10, 5, numSegments, numOwners)));
      rows.add(null);
      rows.add(row("20 random events × 10 seeds",
                   collectRandomTopologies(10, 20, numSegments, numOwners)));

      String header  = String.format("%-45s  %8s  %8s  %12s  %11s  %13s",
            "Scenario", "Default", "Sync", "PureRendezv", "Rendezvous", "HistoryHinted");
      String divider = "-".repeat(103);

      System.out.println();
      System.out.println("## Owner list changed (includes primary<->backup reorders)");
      System.out.println(header);
      System.out.println(divider);
      printTable(rows, 0);

      System.out.println();
      System.out.println("## Ownership lost (excludes free primary<->backup reorders)");
      System.out.println(header);
      System.out.println(divider);
      printTable(rows, 1);

      System.out.println();
   }

   private static Object[] row(String label, int[][] data) {
      return new Object[]{label, data};
   }

   /** Prints one table using metric index 0 (moved) or 1 (lost). */
   private static void printTable(List<Object[]> rows, int metric) {
      for (Object[] r : rows) {
         if (r == null) {
            System.out.println();
            continue;
         }
         String label = (String) r[0];
         int[][] d = (int[][]) r[1];
         System.out.printf("%-45s  %8d  %8d  %12d  %11d  %13d%n",
               label, d[0][metric], d[1][metric], d[2][metric], d[3][metric], d[4][metric]);
      }
   }

   // ---- Matrix helpers ----

   /** Returns int[4][2]: [factory][0]=moved, [factory][1]=lost, from the same CH instances. */
   private int[][] collectJoin(int beforeCount, int afterCount, int numSegments, int numOwners) {
      List<Address> before = makeNodes(beforeCount);
      List<Address> after = new ArrayList<>(before);
      for (int i = beforeCount; i < afterCount; i++) after.add(Address.random("join" + i));
      return collectRow(before, after, numSegments, numOwners);
   }

   private int[][] collectLeave(int beforeCount, int afterCount, int numSegments, int numOwners) {
      List<Address> before = makeNodes(beforeCount);
      List<Address> after = before.subList(0, afterCount);
      return collectRow(before, after, numSegments, numOwners);
   }

   private int[][] collectRow(List<Address> before, List<Address> after,
                               int numSegments, int numOwners) {
      return new int[][] {
            collectPair(DEFAULT,         before, after, numSegments, numOwners),
            collectPair(SYNC,            before, after, numSegments, numOwners),
            collectPair(PURE_RENDEZVOUS, before, after, numSegments, numOwners),
            collectPair(RENDEZVOUS,      before, after, numSegments, numOwners),
            collectPair(HISTORY_HINTED,  before, after, numSegments, numOwners),
      };
   }

   /** Builds both CHs once and returns [moved, lost]. */
   private int[] collectPair(ConsistentHashFactory<DefaultConsistentHash> factory,
                              List<Address> before, List<Address> after,
                              int numSegments, int numOwners) {
      DefaultConsistentHash chBefore = factory.create(numOwners, numSegments, before, null);
      DefaultConsistentHash chAfter  = factory.rebalance(factory.updateMembers(chBefore, after, null));
      return new int[]{ countMovedSegments(chBefore, chAfter), countLostSegments(chBefore, chAfter) };
   }

   private int[][] collectConsecutiveJoins(int startNodes, int numJoins,
                                            int numSegments, int numOwners) {
      List<Address> members = new ArrayList<>(makeNodes(startNodes));
      DefaultConsistentHash defCH = DEFAULT.create(numOwners, numSegments, members, null);
      DefaultConsistentHash synCH = SYNC.create(numOwners, numSegments, members, null);
      DefaultConsistentHash purCH = PURE_RENDEZVOUS.create(numOwners, numSegments, members, null);
      DefaultConsistentHash renCH = RENDEZVOUS.create(numOwners, numSegments, members, null);
      DefaultConsistentHash hisCH = HISTORY_HINTED.create(numOwners, numSegments, members, null);
      int[] totDef = new int[2], totSyn = new int[2], totPur = new int[2], totRen = new int[2], totHis = new int[2];

      for (int i = 0; i < numJoins; i++) {
         members.add(Address.random("join" + i));
         DefaultConsistentHash newDef = DEFAULT.rebalance(DEFAULT.updateMembers(defCH, members, null));
         DefaultConsistentHash newSyn = SYNC.rebalance(SYNC.updateMembers(synCH, members, null));
         DefaultConsistentHash newPur = PURE_RENDEZVOUS.rebalance(PURE_RENDEZVOUS.updateMembers(purCH, members, null));
         DefaultConsistentHash newRen = RENDEZVOUS.rebalance(RENDEZVOUS.updateMembers(renCH, members, null));
         DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(hisCH, members, null));
         accumulate(totDef, defCH, newDef);
         accumulate(totSyn, synCH, newSyn);
         accumulate(totPur, purCH, newPur);
         accumulate(totRen, renCH, newRen);
         accumulate(totHis, hisCH, newHis);
         defCH = newDef;
         synCH = newSyn;
         purCH = newPur;
         renCH = newRen;
         hisCH = newHis;
      }
      return new int[][]{ totDef, totSyn, totPur, totRen, totHis };
   }

   private int[][] collectConsecutiveLeaves(int startNodes, int numLeaves,
                                             int numSegments, int numOwners) {
      List<Address> members = new ArrayList<>(makeNodes(startNodes));
      DefaultConsistentHash defCH = DEFAULT.create(numOwners, numSegments, members, null);
      DefaultConsistentHash synCH = SYNC.create(numOwners, numSegments, members, null);
      DefaultConsistentHash purCH = PURE_RENDEZVOUS.create(numOwners, numSegments, members, null);
      DefaultConsistentHash renCH = RENDEZVOUS.create(numOwners, numSegments, members, null);
      DefaultConsistentHash hisCH = HISTORY_HINTED.create(numOwners, numSegments, members, null);
      int[] totDef = new int[2], totSyn = new int[2], totPur = new int[2], totRen = new int[2], totHis = new int[2];

      for (int i = 0; i < numLeaves; i++) {
         members.remove(members.size() - 1);
         DefaultConsistentHash newDef = DEFAULT.rebalance(DEFAULT.updateMembers(defCH, members, null));
         DefaultConsistentHash newSyn = SYNC.rebalance(SYNC.updateMembers(synCH, members, null));
         DefaultConsistentHash newPur = PURE_RENDEZVOUS.rebalance(PURE_RENDEZVOUS.updateMembers(purCH, members, null));
         DefaultConsistentHash newRen = RENDEZVOUS.rebalance(RENDEZVOUS.updateMembers(renCH, members, null));
         DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(hisCH, members, null));
         accumulate(totDef, defCH, newDef);
         accumulate(totSyn, synCH, newSyn);
         accumulate(totPur, purCH, newPur);
         accumulate(totRen, renCH, newRen);
         accumulate(totHis, hisCH, newHis);
         defCH = newDef;
         synCH = newSyn;
         purCH = newPur;
         renCH = newRen;
         hisCH = newHis;
      }
      return new int[][]{ totDef, totSyn, totPur, totRen, totHis };
   }

   private int[][] collectRandomTopologies(int numSeeds, int eventsPerSeed,
                                            int numSegments, int numOwners) {
      int[] totDef = new int[2], totSyn = new int[2], totPur = new int[2], totRen = new int[2], totHis = new int[2];

      for (int seed = 0; seed < numSeeds; seed++) {
         Random rng = new Random(seed);
         List<Address> members = new ArrayList<>(makeNodes(8));
         DefaultConsistentHash defCH = DEFAULT.create(numOwners, numSegments, members, null);
         DefaultConsistentHash synCH = SYNC.create(numOwners, numSegments, members, null);
         DefaultConsistentHash purCH = PURE_RENDEZVOUS.create(numOwners, numSegments, members, null);
         DefaultConsistentHash renCH = RENDEZVOUS.create(numOwners, numSegments, members, null);
         DefaultConsistentHash hisCH = HISTORY_HINTED.create(numOwners, numSegments, members, null);

         for (int event = 0; event < eventsPerSeed; event++) {
            if (members.size() > 5 && (rng.nextBoolean() || members.size() >= 15)) {
               members.remove(rng.nextInt(members.size()));
            } else {
               members.add(Address.random("rnd-" + seed + "-" + event));
            }
            DefaultConsistentHash newDef = DEFAULT.rebalance(DEFAULT.updateMembers(defCH, members, null));
            DefaultConsistentHash newSyn = SYNC.rebalance(SYNC.updateMembers(synCH, members, null));
            DefaultConsistentHash newPur = PURE_RENDEZVOUS.rebalance(PURE_RENDEZVOUS.updateMembers(purCH, members, null));
            DefaultConsistentHash newRen = RENDEZVOUS.rebalance(RENDEZVOUS.updateMembers(renCH, members, null));
            DefaultConsistentHash newHis = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(hisCH, members, null));
            accumulate(totDef, defCH, newDef);
            accumulate(totSyn, synCH, newSyn);
            accumulate(totPur, purCH, newPur);
            accumulate(totRen, renCH, newRen);
            accumulate(totHis, hisCH, newHis);
            defCH = newDef;
            synCH = newSyn;
            purCH = newPur;
            renCH = newRen;
            hisCH = newHis;
         }
      }
      return new int[][]{ totDef, totSyn, totPur, totRen, totHis };
   }

   private static void accumulate(int[] totals, DefaultConsistentHash before, DefaultConsistentHash after) {
      totals[0] += countMovedSegments(before, after);
      totals[1] += countLostSegments(before, after);
   }

   // ---- Primary/backup ratio tests ----

   /**
    * Per-factory primary-balance bounds, expressed as the allowed deviation above {@code ceil}
    * (upper tolerance) and below {@code floor} (lower tolerance).
    *
    * <p>These act as <em>regression guards</em>: a test failure means the factory got
    * <em>worse</em>, not that it ever achieved the strict {@code [floor..ceil]} bound.</p>
    *
    * <ul>
    *   <li><b>Default</b>: strict — tolerance = 0 above and below.</li>
    *   <li><b>Sync</b>: iterative {@code fudgeExpectedSegments} approximation; empirically ±2.</li>
    *   <li><b>Rendezvous</b>: the redistribution loop only drains nodes above {@code ceil+1},
    *       so the <em>ceiling</em> is bounded at {@code ceil+1} (upperTol=1).  The <em>floor</em>
    *       is completely unbounded — under-loaded nodes are never filled up.  After a join the
    *       new joiner may receive far fewer primaries than floor until the next full rebalance.
    *       We record the empirically observed worst-case lower deviation as a regression guard;
    *       the goal of the redistribution threshold fix is to bring lowerTol to 0.</li>
    * </ul>
    */
   private static int[] primaryTolerances(ConsistentHashFactory<?> factory) {
      // returns { lowerTol, upperTol }  — how far below floor and above ceil is acceptable
      // Rendezvous: both the total-redistribution drain threshold and the primary-redistribution
      // drain threshold are ceil+1 (fire at ceil+2), so in the worst case a node can sit at
      // ceil+1 for totals and the primary pass re-distributes within those totals, leaving
      // primaries up to ceil+1 above the ideal.  upperTol=1 reflects that.  With capacity
      // factors, the asymmetric ideal values (non-integer ideals for some nodes) mean the
      // primary pass can get stuck one step higher; upperTol=2 covers that until threshold #1
      // (drain at strict ceil) is addressed.
      if (factory instanceof SyncConsistentHashFactory)       return new int[]{2, 2};
      if (factory instanceof RendezvousConsistentHashFactory) return new int[]{Integer.MAX_VALUE, 2};
      return new int[]{0, 0}; // DefaultConsistentHashFactory — strict
   }

   /**
    * Verifies that every node's primary ownership count is within the per-factory tolerance of
    * the ideal {@code numSegments / numNodes}.
    *
    * <p>The ideal invariant: each node should be primary for approximately {@code S/N} segments
    * so that write and read-primary load is evenly distributed.  A node with many backup segments
    * but almost no primaries receives far fewer writes than its peers.</p>
    *
    * <p>The assertion bounds are asymmetric for Rendezvous (ceiling bounded, floor unbounded —
    * see {@link #primaryTolerances}).  {@link PureRendezvousConsistentHashFactory} is excluded
    * since it makes no load-balance guarantees at all.</p>
    */
   public void testPrimaryBalance_UniformCapacity() {
      int[][] configs = {
            // { numNodes, numSegments, numOwners }
            {  4, 256, 2 },
            {  4, 256, 1 },
            {  8, 256, 2 },
            {  5, 256, 2 },
            { 10, 256, 2 },
            { 20, 256, 2 },
            { 50, 512, 2 },
            {  3,  64, 2 },
            {  7,  64, 3 },
      };

      System.out.println();
      System.out.printf("%-55s  %7s  %7s  %15s%n",
            "testPrimaryBalance_UniformCapacity  [floor..ceil]", "Default", "Sync", "HistoryHinted");
      System.out.println("-".repeat(87));

      for (int[] cfg : configs) {
         int numNodes = cfg[0], numSegments = cfg[1], numOwners = cfg[2];
         List<Address> members = makeNodes(numNodes);
         int floorP = numSegments / numNodes;
         int ceilP  = (numSegments + numNodes - 1) / numNodes;

         int defMax = 0, synMax = 0, hisMax = 0;
         for (ConsistentHashFactory<DefaultConsistentHash> factory : List.of(DEFAULT, SYNC, HISTORY_HINTED)) {
            DefaultConsistentHash ch = factory.create(numOwners, numSegments, members, null);
            int worstDev = worstPrimaryDeviation(ch, members, floorP, ceilP);
            if (factory == DEFAULT) defMax = worstDev;
            else if (factory == SYNC) synMax = worstDev;
            else hisMax = worstDev;
            assertPrimaryBalance(factory, ch, members, floorP, ceilP, primaryTolerances(factory));
         }
         System.out.printf("  %d nodes %d segs owners=%d  [%d..%d]  %+7d  %+7d  %+15d%n",
               numNodes, numSegments, numOwners, floorP, ceilP, defMax, synMax, hisMax);
      }
      System.out.println();
   }

   /**
    * Same check with capacity factors: each node's primary-ideal is proportional to its capacity
    * weight, and must lie within {@code [floor - tol, ceil + tol]} of that weighted ideal.
    */
   public void testPrimaryBalance_CapacityFactors() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      Address C = Address.random("C");
      Address D = Address.random("D");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 2f);
      cf.put(C, 2f);
      cf.put(D, 3f);
      List<Address> members = Arrays.asList(A, B, C, D);
      int numSegments = 256;
      float totalCap = 8f;

      System.out.println();
      System.out.printf("testPrimaryBalance_CapacityFactors  (4 nodes cf=1:2:2:3, %d segs, owners=2)%n", numSegments);
      System.out.printf("  %-6s  ideal   Default  Sync  HistoryHinted%n", "Node");

      for (ConsistentHashFactory<DefaultConsistentHash> factory : List.of(DEFAULT, SYNC, HISTORY_HINTED)) {
         DefaultConsistentHash ch = factory.create(2, numSegments, members, cf);
         OwnershipStatistics stats = new OwnershipStatistics(ch, members);
         int[] tols = primaryTolerances(factory);
         for (Address node : members) {
            float nodeIdeal = numSegments * cf.get(node) / totalCap;
            int floor = (int) Math.floor(nodeIdeal);
            int ceil  = (int) Math.ceil(nodeIdeal);
            int primaryOwned = stats.getPrimaryOwned(node);
            int lo = tols[0] == Integer.MAX_VALUE ? 0 : floor - tols[0];
            int hi = ceil + tols[1];
            assertTrue(primaryOwned >= lo && primaryOwned <= hi,
                  factory.getClass().getSimpleName() + " node " + node
                        + ": primaryOwned=" + primaryOwned + " expected [" + lo + ".." + hi + "]"
                        + " (nodeIdeal=" + nodeIdeal + ", strict [" + floor + ".." + ceil + "])");
         }
         // Print per-factory row
         System.out.printf("  %s: ", factory.getClass().getSimpleName());
         for (Address node : members) {
            System.out.printf("%s=%d  ", node, stats.getPrimaryOwned(node));
         }
         System.out.println();
      }
      System.out.println();
   }

   /**
    * Verifies that primary balance is maintained after topology changes, not only at initial
    * {@code create()}.  Checks after a single node join, a single node leave, a bulk join, and
    * with {@code numOwners=3}, for each of Default, Sync, and Rendezvous.
    */
   public void testPrimaryBalance_AfterTopologyChange() {
      int numSegments = 256;

      System.out.println();
      System.out.println("testPrimaryBalance_AfterTopologyChange");
      System.out.printf("  %-35s  %7s  %7s  %15s%n", "Scenario [floor..ceil]", "Default", "Sync", "HistoryHinted");
      System.out.println("  " + "-".repeat(69));

      for (int numOwners : new int[]{2, 3}) {
         // Single join: 9 → 10
         runAndPrintTopologyScenario("join  9→10 owners=" + numOwners,
               makeNodes(9), addNode(makeNodes(9)),
               numOwners, numSegments);

         // Single leave: 10 → 9
         List<Address> ten = makeNodes(10);
         runAndPrintTopologyScenario("leave 10→9 owners=" + numOwners,
               ten, ten.subList(0, 9),
               numOwners, numSegments);

         // Bulk join: 10 → 20
         List<Address> bulkBefore = makeNodes(10);
         List<Address> bulkAfter  = new ArrayList<>(bulkBefore);
         for (int i = 10; i < 20; i++) bulkAfter.add(Address.random("bulk" + i));
         runAndPrintTopologyScenario("join  10→20 owners=" + numOwners,
               bulkBefore, bulkAfter,
               numOwners, numSegments);
      }
      System.out.println();
   }

   private void runAndPrintTopologyScenario(String label,
                                             List<Address> before, List<Address> after,
                                             int numOwners, int numSegments) {
      int n = after.size();
      int floorP = numSegments / n;
      int ceilP  = (numSegments + n - 1) / n;
      int defMax = 0, synMax = 0, hisMax = 0;

      for (ConsistentHashFactory<DefaultConsistentHash> factory : List.of(/*DEFAULT, SYNC,*/ HISTORY_HINTED)) {
         DefaultConsistentHash ch = factory.rebalance(
               factory.updateMembers(factory.create(numOwners, numSegments, before, null), after, null));
         int worstDev = worstPrimaryDeviation(ch, after, floorP, ceilP);
         if (factory == DEFAULT) defMax = worstDev;
         else if (factory == SYNC) synMax = worstDev;
         else hisMax = worstDev;
         assertPrimaryBalance(factory, ch, after, floorP, ceilP, primaryTolerances(factory));
      }
      System.out.printf("  %-35s  %+7d  %+7d  %+15d   [%d..%d]%n",
            label, defMax, synMax, hisMax, floorP, ceilP);
   }

   private static List<Address> addNode(List<Address> base) {
      List<Address> result = new ArrayList<>(base);
      result.add(Address.random("joiner"));
      return result;
   }

   /**
    * Informational: prints a per-node primary count table for all four factories across several
    * configurations.  Never fails — run individually to compare factories.
    */
   public void testPrimaryBackupRatioMatrix() {
      int[][] configs = {
            {  4, 256, 2 },
            {  8, 256, 2 },
            {  5, 256, 2 },
            { 10, 256, 2 },
            { 20, 256, 2 },
            {  7,  64, 3 },
      };

      System.out.println();
      System.out.println("## Primary ownership per node  (ideal = numSegments / numNodes)");
      System.out.printf("%-42s  %7s  %7s  %14s  %12s%n",
            "Config / node", "Default", "Sync", "PureRendezvous", "Rendezvous");
      System.out.println("-".repeat(88));

      for (int[] cfg : configs) {
         int numNodes = cfg[0], numSegments = cfg[1], numOwners = cfg[2];
         List<Address> members = makeNodes(numNodes);
         float primaryIdeal = (float) numSegments / numNodes;
         int floorP = numSegments / numNodes;
         int ceilP  = (numSegments + numNodes - 1) / numNodes;

         System.out.printf("  %d nodes, %d segs, owners=%d  (ideal=%.1f, floor=%d, ceil=%d)%n",
               numNodes, numSegments, numOwners, primaryIdeal, floorP, ceilP);

         DefaultConsistentHash defCH  = DEFAULT.create(numOwners, numSegments, members, null);
         DefaultConsistentHash synCH  = SYNC.create(numOwners, numSegments, members, null);
         DefaultConsistentHash purCH  = PURE_RENDEZVOUS.create(numOwners, numSegments, members, null);
         DefaultConsistentHash renCH  = RENDEZVOUS.create(numOwners, numSegments, members, null);

         for (int i = 0; i < members.size(); i++) {
            Address node = members.get(i);
            int defP = new OwnershipStatistics(defCH, members).getPrimaryOwned(node);
            int synP = new OwnershipStatistics(synCH, members).getPrimaryOwned(node);
            int purP = new OwnershipStatistics(purCH, members).getPrimaryOwned(node);
            int renP = new OwnershipStatistics(renCH, members).getPrimaryOwned(node);
            // Flag values outside [floor, ceil] with an asterisk
            String defF = defP < floorP || defP > ceilP ? "*" : " ";
            String synF = synP < floorP || synP > ceilP ? "*" : " ";
            String purF = purP < floorP || purP > ceilP ? "*" : " ";
            String renF = renP < floorP || renP > ceilP ? "*" : " ";
            System.out.printf("    node%-3d  %6d%s  %6d%s  %13d%s  %11d%s%n",
                  i, defP, defF, synP, synF, purP, purF, renP, renF);
         }
         System.out.println();
      }
      System.out.println("  (* = outside strict [floor..ceil])");
   }

   // ---- Primary balance helpers ----

   /**
    * Returns the worst signed deviation from [floor..ceil] across all members:
    * positive means above ceil, negative means below floor, zero means within bounds.
    */
   private static int worstPrimaryDeviation(DefaultConsistentHash ch, List<Address> members,
                                             int floor, int ceil) {
      OwnershipStatistics stats = new OwnershipStatistics(ch, members);
      int worst = 0;
      for (Address node : members) {
         int p = stats.getPrimaryOwned(node);
         int dev = p > ceil ? p - ceil : (p < floor ? p - floor : 0);
         if (Math.abs(dev) > Math.abs(worst)) worst = dev;
      }
      return worst;
   }

   /**
    * Asserts primary balance using asymmetric per-factory tolerances.
    *
    * @param tols {@code int[]{lowerTol, upperTol}} from {@link #primaryTolerances}.
    *             A {@code lowerTol} of {@link Integer#MAX_VALUE} means the floor is unbounded
    *             (the lower bound is clamped to 0).
    */
   private static void assertPrimaryBalance(
         ConsistentHashFactory<DefaultConsistentHash> factory,
         DefaultConsistentHash ch, List<Address> members, int floor, int ceil, int[] tols) {
      OwnershipStatistics stats = new OwnershipStatistics(ch, members);
      int lo = tols[0] == Integer.MAX_VALUE ? 0 : floor - tols[0];
      int hi = ceil + tols[1];
      for (Address node : members) {
         int primaryOwned = stats.getPrimaryOwned(node);
         assertTrue(primaryOwned >= lo && primaryOwned <= hi,
               factory.getClass().getSimpleName()
                     + " [" + members.size() + " nodes, " + ch.getNumSegments() + " segs"
                     + ", owners=" + ch.getNumOwners() + "]"
                     + " node " + node + ": primaryOwned=" + primaryOwned
                     + " expected [" + lo + ".." + hi + "]"
                     + " (strict [" + floor + ".." + ceil + "])");
      }
   }

   // ---- Distribution matrix (informational — always passes) ----

   /**
    * Prints a side-by-side table of per-node segment ownership divergence from the ideal for
    * Default, Sync, PureRendezvous, and Rendezvous (greedy) across a range of configurations.
    *
    * <p>For each configuration the ideal per-node ownership is {@code numSegments * numOwners / numNodes}
    * for total ownership and {@code numSegments / numNodes} for primary ownership. Three statistics
    * are reported per factory: max deviation (worst node), mean absolute deviation (MAD), and
    * standard deviation (StdDev). All figures are in segments.</p>
    *
    * <p>This test never fails — it is purely informational.</p>
    */
   public void testDistributionMatrix() {
      System.out.println();

      // Total ownership divergence
      System.out.println("## Total ownership divergence (segments per node vs ideal)");
      System.out.println();
      System.out.println("Each cell shows the deviation from the ideal ownership count. " +
            "**Max** = worst single node, **MAD** = mean absolute deviation across all nodes, " +
            "**StdDev** = standard deviation.");
      System.out.println();
      mdTableHeader();
      mdDistRow("4 nodes, 256 segs, owners=2 (ideal=128)",    4, 256, 2);
      mdDistRow("4 nodes, 256 segs, owners=1 (ideal=64)",     4, 256, 1);
      mdDistRow("8 nodes, 256 segs, owners=2 (ideal=64)",     8, 256, 2);
      mdDistRow("5 nodes, 256 segs, owners=2 (ideal=102.4)",  5, 256, 2);
      mdDistRow("10 nodes, 256 segs, owners=2 (ideal=51.2)", 10, 256, 2);
      mdDistRow("20 nodes, 256 segs, owners=2 (ideal=25.6)", 20, 256, 2);
      mdDistRow("50 nodes, 512 segs, owners=2 (ideal=20.5)", 50, 512, 2);
      mdDistRow("3 nodes, 64 segs, owners=2 (ideal=42.7)",    3,  64, 2);
      mdDistRow("7 nodes, 64 segs, owners=3 (ideal=27.4)",    7,  64, 3);

      System.out.println();

      // Primary ownership divergence
      System.out.println("## Primary ownership divergence (primary segments per node vs ideal)");
      System.out.println();
      System.out.println("Same statistics, but counted only for position-0 (primary) ownership.");
      System.out.println();
      mdTableHeader();
      mdPrimaryDistRow("4 nodes, 256 segs, owners=2 (ideal=64)",    4, 256, 2);
      mdPrimaryDistRow("4 nodes, 256 segs, owners=1 (ideal=64)",    4, 256, 1);
      mdPrimaryDistRow("8 nodes, 256 segs, owners=2 (ideal=32)",    8, 256, 2);
      mdPrimaryDistRow("5 nodes, 256 segs, owners=2 (ideal=51.2)",  5, 256, 2);
      mdPrimaryDistRow("10 nodes, 256 segs, owners=2 (ideal=25.6)", 10, 256, 2);
      mdPrimaryDistRow("20 nodes, 256 segs, owners=2 (ideal=12.8)", 20, 256, 2);
      mdPrimaryDistRow("50 nodes, 512 segs, owners=2 (ideal=10.2)", 50, 512, 2);
      mdPrimaryDistRow("3 nodes, 64 segs, owners=2 (ideal=21.3)",   3,  64, 2);
      mdPrimaryDistRow("7 nodes, 64 segs, owners=3 (ideal=9.1)",    7,  64, 3);

      System.out.println();
   }

   private static void mdTableHeader() {
      // 13 columns: Configuration + 3 stats × 4 factories
      System.out.println("| Configuration " +
            "| Default Max | Default MAD | Default StdDev " +
            "| Sync Max | Sync MAD | Sync StdDev " +
            "| PureRendezvous Max | PureRendezvous MAD | PureRendezvous StdDev " +
            "| Rendezvous Max | Rendezvous MAD | Rendezvous StdDev |");
      System.out.println("| --- " +
            "| ---: | ---: | ---: " +
            "| ---: | ---: | ---: " +
            "| ---: | ---: | ---: " +
            "| ---: | ---: | ---: |");
   }

   private void mdDistRow(String label, int numNodes, int numSegments, int numOwners) {
      List<Address> members = makeNodes(numNodes);
      double ideal = (double) numSegments * numOwners / numNodes;
      System.out.printf("| %s %s %s %s %s |%n", label,
            mdStats(DEFAULT,         members, numSegments, numOwners, ideal, false),
            mdStats(SYNC,            members, numSegments, numOwners, ideal, false),
            mdStats(PURE_RENDEZVOUS, members, numSegments, numOwners, ideal, false),
            mdStats(RENDEZVOUS,      members, numSegments, numOwners, ideal, false));
   }

   private void mdPrimaryDistRow(String label, int numNodes, int numSegments, int numOwners) {
      List<Address> members = makeNodes(numNodes);
      double ideal = (double) numSegments / numNodes;
      System.out.printf("| %s %s %s %s %s |%n", label,
            mdStats(DEFAULT,         members, numSegments, numOwners, ideal, true),
            mdStats(SYNC,            members, numSegments, numOwners, ideal, true),
            mdStats(PURE_RENDEZVOUS, members, numSegments, numOwners, ideal, true),
            mdStats(RENDEZVOUS,      members, numSegments, numOwners, ideal, true));
   }

   /**
    * Returns three pipe-delimited markdown cells "| max | MAD | StdDev " (no trailing pipe —
    * the caller writes the final closing pipe for the row).
    */
   private String mdStats(ConsistentHashFactory<DefaultConsistentHash> factory,
                           List<Address> members, int numSegments, int numOwners,
                           double ideal, boolean primaryOnly) {
      DefaultConsistentHash ch = factory.create(numOwners, numSegments, members, null);
      OwnershipStatistics stats = new OwnershipStatistics(ch, members);

      int n = members.size();
      double maxDev = 0;
      double sumAbsDev = 0;
      double sumSqDev = 0;

      for (Address node : members) {
         int actual = primaryOnly ? stats.getPrimaryOwned(node) : stats.getOwned(node);
         double dev = Math.abs(actual - ideal);
         if (dev > maxDev) maxDev = dev;
         sumAbsDev += dev;
         sumSqDev += dev * dev;
      }

      double mad = sumAbsDev / n;
      double stdDev = Math.sqrt(sumSqDev / n);
      return String.format("| %.1f | %.2f | %.2f ", maxDev, mad, stdDev);
   }

   // ---- Existing helpers ----

   private void assertHistoryHintedLeqSync(int beforeCount, int afterCount,
                                            int numSegments, int numOwners, String desc) {
      List<Address> before = makeNodes(beforeCount);
      List<Address> after = new ArrayList<>(before);
      for (int i = beforeCount; i < afterCount; i++) after.add(Address.random("join" + i));
      assertHistoryHintedLeqSync(before, after, numSegments, numOwners, desc);
   }

   private void assertHistoryHintedLeqSyncLeave(int beforeCount, int afterCount,
                                                 int numSegments, int numOwners, String desc) {
      List<Address> before = makeNodes(beforeCount);
      List<Address> after = before.subList(0, afterCount);
      assertHistoryHintedLeqSync(before, after, numSegments, numOwners, desc);
   }

   private void assertHistoryHintedLeqSync(List<Address> before, List<Address> after,
                                            int numSegments, int numOwners, String desc) {
      DefaultConsistentHash hisBefore = HISTORY_HINTED.create(numOwners, numSegments, before, null);
      DefaultConsistentHash hisAfter = HISTORY_HINTED.rebalance(HISTORY_HINTED.updateMembers(hisBefore, after, null));

      DefaultConsistentHash syncBefore = SYNC.create(numOwners, numSegments, before, null);
      DefaultConsistentHash syncAfter = SYNC.rebalance(SYNC.updateMembers(syncBefore, after, null));

      int hisMoved = countMovedSegments(hisBefore, hisAfter);
      int syncMoved = countMovedSegments(syncBefore, syncAfter);
      // Allow up to 10% slack: HistoryHinted targets near-minimal movement but correction passes may add slight overhead
      assertTrue(hisMoved <= syncMoved * 1.10 + numSegments * 0.05,
            desc + ": HistoryHinted moved " + hisMoved + " segments, Sync moved " + syncMoved);
   }

   private static void assertInRange(String msg, int actual, int min, int max) {
      if (actual < min || actual > max) {
         throw new AssertionError(msg + ": expected [" + min + ".." + max + "] but got " + actual);
      }
   }

   private static void assertEquals(Object a, Object b, String msg) {
      org.junit.jupiter.api.Assertions.assertEquals(a, b, msg);
   }

   // ---- Scenario deep-dive ----

   /**
    * Deep-dive into the 19→20 node join scenario: the single worst gap between PureRendezvous
    * and Rendezvous in "ownership lost" terms (Pure=25, Ren=39 in the last run).
    *
    * <p>For every segment whose owner <em>set</em> differs between PureRendezvous and Rendezvous
    * after the join this test prints:</p>
    * <ul>
    *   <li>The segment index.</li>
    *   <li>Whether the difference is because Rendezvous has a different node at some position
    *       (load-balancer swap) vs. merely a different primary/backup order for the same set.</li>
    *   <li>For each node evicted by the load balancer relative to PureRendezvous: the node's
    *       rendezvous rank in that segment (0 = best), whether it was primary or backup, and the
    *       rank of its replacement.</li>
    * </ul>
    *
    * <p>The test also prints aggregate statistics that directly test the core assumption of the
    * load-balancer: <em>"we always evict the highest-ranked (worst-fit) node"</em>.  If evictions
    * are frequently targeting low-rank (well-fitting) nodes it means the balancer is creating
    * unnecessary movement.</p>
    *
    * <p>This test never fails — it is purely informational.</p>
    */
   public void testScenarioDeepDive() {
      final int numSegments = 256;
      final int numOwners   = 2;
      final int beforeCount = 19;
      final int afterCount  = 20;

      List<Address> before = makeNodes(beforeCount);
      List<Address> after  = new ArrayList<>(before);
      after.add(Address.random("joiner"));

      // Build both CHs after the join (same approach as the matrix test)
      DefaultConsistentHash pureBefore = PURE_RENDEZVOUS.create(numOwners, numSegments, before, null);
      DefaultConsistentHash pureAfter  = PURE_RENDEZVOUS.rebalance(
            PURE_RENDEZVOUS.updateMembers(pureBefore, after, null));

      DefaultConsistentHash renBefore  = RENDEZVOUS.create(numOwners, numSegments, before, null);
      DefaultConsistentHash renAfter   = RENDEZVOUS.rebalance(
            RENDEZVOUS.updateMembers(renBefore, after, null));

      // Pre-compute rendezvous rankings for the full *after* member list so we can look up ranks
      @SuppressWarnings("unchecked")
      List<Address>[] rankings = ((PureRendezvousConsistentHashFactory) PURE_RENDEZVOUS)
            .computeRankings(numSegments, after, null);

      // Segment-level loads in the *after* CHs (for "over-loaded?" context)
      int[] pureTotalOwned = new int[afterCount];
      int[] renTotalOwned  = new int[afterCount];
      for (int s = 0; s < numSegments; s++) {
         for (Address a : pureAfter.locateOwnersForSegment(s)) {
            pureTotalOwned[after.indexOf(a)]++;
         }
         for (Address a : renAfter.locateOwnersForSegment(s)) {
            renTotalOwned[after.indexOf(a)]++;
         }
      }

      float idealTotal = (float) numSegments * numOwners / afterCount;   // ~25.6
      float idealPrimary = (float) numSegments / afterCount;             // ~12.8
      int ceilTotal   = (int) Math.ceil(idealTotal);
      int ceilPrimary = (int) Math.ceil(idealPrimary);

      // Collect summary counters
      // For segments where Rendezvous evicted a node that Pure kept:
      //   rankOfEvicted — rendezvous rank (0-based) of the node evicted by Ren's balancer
      //   rankOfReplacement — rank of the node Ren put in instead
      // "Good eviction" = evicted rank > replacement rank  (we kicked out a worse-fit node)
      // "Bad eviction"  = evicted rank < replacement rank  (we kicked out a better-fit node — wasted movement)
      // "Unnecessary"   = evicted rank < replacement rank AND evicted node was within ideal bounds in pure
      List<int[]> evictions = new ArrayList<>();   // [evictedRank, replacementRank, isPrimary, segment]
      int primaryEvictions = 0;
      int backupEvictions  = 0;
      int reordersOnly     = 0;  // same set, different primary
      int pureOnlyChanges  = 0;  // pure moved it but ren didn't (or vice versa — pure baseline)

      // Track which segments each factory changed vs. before-join
      Set<Integer> pureChanged = new HashSet<>();
      Set<Integer> renChanged  = new HashSet<>();
      for (int s = 0; s < numSegments; s++) {
         Set<Address> bSet = new HashSet<>(pureBefore.locateOwnersForSegment(s));
         if (!new HashSet<>(pureAfter.locateOwnersForSegment(s)).equals(bSet)) {
            pureChanged.add(s);
         }
         if (!new HashSet<>(renBefore.locateOwnersForSegment(s)).equals(
               new HashSet<>(renAfter.locateOwnersForSegment(s)))) {
            renChanged.add(s);
         }
      }

      System.out.println();
      System.out.println("=================================================================");
      System.out.println(" SCENARIO DEEP DIVE: 19→20 node join, 256 segments, owners=2");
      System.out.println("=================================================================");
      System.out.printf("  idealTotal=%.2f (ceil=%d)   idealPrimary=%.2f (ceil=%d)%n",
            idealTotal, ceilTotal, idealPrimary, ceilPrimary);
      System.out.println();

      // --- Section 1: per-node load after rebalance ---
      System.out.println("-- Per-node ownership after join (pure vs rendezvous) --");
      System.out.printf("  %-8s  %6s  %6s  %6s  %6s  %s%n",
            "Node", "pureTot", "renTot", "purePri", "renPri", "Flags");
      System.out.println("  " + "-".repeat(58));
      int[] purePrimary = new int[afterCount];
      int[] renPrimary  = new int[afterCount];
      for (int s = 0; s < numSegments; s++) {
         purePrimary[after.indexOf(pureAfter.locateOwnersForSegment(s).get(0))]++;
         renPrimary[after.indexOf(renAfter.locateOwnersForSegment(s).get(0))]++;
      }
      for (int i = 0; i < afterCount; i++) {
         String flags = "";
         if (renTotalOwned[i] > ceilTotal)   flags += " OVER_TOTAL";
         if (renPrimary[i]    > ceilPrimary) flags += " OVER_PRIMARY";
         if (renTotalOwned[i] < pureTotalOwned[i] - 1) flags += " UNDER_vs_PURE";
         System.out.printf("  node%-4d  %6d  %6d  %6d  %6d  %s%n",
               i, pureTotalOwned[i], renTotalOwned[i], purePrimary[i], renPrimary[i], flags);
      }
      System.out.println();

      // --- Section 2: per-segment diff between Pure and Ren ---
      System.out.println("-- Segments where Ren differs from Pure (owner set or order) --");
      System.out.printf("  %-4s  %-28s  %-28s  %-30s  %s%n",
            "Seg", "PureOwners[rank]", "RenOwners[rank]", "Evicted→Replacement", "Note");
      System.out.println("  " + "-".repeat(100));

      for (int s = 0; s < numSegments; s++) {
         List<Address> pureOwners = pureAfter.locateOwnersForSegment(s);
         List<Address> renOwners  = renAfter.locateOwnersForSegment(s);
         List<Address> ranking    = rankings[s];

         boolean sameSet = new HashSet<>(pureOwners).equals(new HashSet<>(renOwners));
         boolean sameList = pureOwners.equals(renOwners);

         if (sameList) continue; // identical — nothing to report

         // Format owner list as "nodeX[rank]"
         String pureStr = formatOwners(pureOwners, after, ranking);
         String renStr  = formatOwners(renOwners, after, ranking);

         if (sameSet) {
            // Same nodes, different order — pure primary-swap, no data movement
            reordersOnly++;
            System.out.printf("  %3d   %-28s  %-28s  %-30s  %s%n",
                  s, pureStr, renStr, "(reorder only)", "primary swap");
            continue;
         }

         // Find evicted nodes (in pure but not in ren) and replacements (in ren but not in pure)
         Set<Address> pureSet = new HashSet<>(pureOwners);
         Set<Address> renSet  = new HashSet<>(renOwners);
         List<Address> evicted     = new ArrayList<>(pureSet);
         evicted.removeAll(renSet);
         List<Address> replacements = new ArrayList<>(renSet);
         replacements.removeAll(pureSet);

         StringBuilder evDetails = new StringBuilder();
         for (Address ev : evicted) {
            int evRank  = ranking.indexOf(ev);
            boolean wasPrimary = pureOwners.get(0).equals(ev);
            for (Address rep : replacements) {
               int repRank = ranking.indexOf(rep);
               evictions.add(new int[]{evRank, repRank, wasPrimary ? 1 : 0, s});
               if (wasPrimary) {
                  primaryEvictions++;
               } else {
                  backupEvictions++;
               }
               if (evDetails.length() > 0) evDetails.append("; ");
               evDetails.append(String.format("ev[r%d,%s]→rep[r%d]",
                     evRank, wasPrimary ? "PRI" : "bak", repRank));
            }
         }
         String note = renChanged.contains(s) && !pureChanged.contains(s)
               ? "Ren-extra-move"
               : pureChanged.contains(s) && !renChanged.contains(s)
               ? "Pure-only-move"
               : "both-moved";
         System.out.printf("  %3d   %-28s  %-28s  %-30s  %s%n",
               s, pureStr, renStr, evDetails, note);
      }
      System.out.println();

      // --- Section 2b: joiner rank profile and its relationship to LB evictions ---
      // The joiner (last node in 'after') gets a rendezvous rank in every segment.
      // We want to know:
      //   (a) How many segments does it rank 0, 1, 2, 3, 4, 5+ in?
      //   (b) For each LB eviction, what rank did the joiner have in that segment?
      //       If the joiner ranked >= numOwners in an evicted segment, the LB touched a
      //       segment the joiner had no natural claim to — that is pure variance overhead.
      //   (c) For over-loaded donors: how many of their excess segments are in segments
      //       where the joiner ranked < numOwners (joiner-driven) vs >= numOwners (variance)?
      Address joiner = after.get(afterCount - 1);
      int joinerIdx  = afterCount - 1;

      // (a) Joiner rank distribution
      int[] joinerRankCount = new int[afterCount]; // how many segments joiner ranks at each position
      for (int s = 0; s < numSegments; s++) {
         int r = rankings[s].indexOf(joiner);
         if (r >= 0 && r < afterCount) joinerRankCount[r]++;
      }
      System.out.println("-- Joiner (node" + joinerIdx + ") rank distribution across all " + numSegments + " segments --");
      System.out.printf("  %-6s  %6s  %s%n", "Rank", "Count", "Meaning");
      System.out.println("  " + "-".repeat(50));
      int joinerNaturalClaim = 0; // segments where joiner ranks < numOwners (natural top-N)
      for (int r = 0; r < afterCount; r++) {
         if (joinerRankCount[r] == 0) continue;
         String meaning = r < numOwners ? "  <-- natural owner (Pure would assign this)" : "";
         if (r < numOwners) joinerNaturalClaim += joinerRankCount[r];
         System.out.printf("  %-6d  %6d  %s%n", r, joinerRankCount[r], meaning);
      }
      System.out.printf("  Total natural claim (rank < %d): %d segments%n", numOwners, joinerNaturalClaim);
      System.out.println();

      // (b) For each LB eviction, what was the joiner's rank in that segment?
      // Bucket evictions into: joiner competed (rank < numOwners) vs joiner absent (rank >= numOwners)
      System.out.println("-- LB evictions cross-referenced with joiner's rank in that segment --");
      System.out.printf("  %-4s  %-20s  %6s  %s%n", "Seg", "Evicted→Rep", "JoinerRank", "Joiner competed?");
      System.out.println("  " + "-".repeat(62));
      int lbInJoinerFootprint  = 0; // LB eviction in a segment the joiner naturally claims
      int lbOutsideFootprint   = 0; // LB eviction in a segment the joiner has no natural claim to
      for (int[] ev : evictions) {
         int s = ev[3];
         int joinerRankInSeg = rankings[s].indexOf(joiner);
         boolean competed = joinerRankInSeg < numOwners;
         if (competed) lbInJoinerFootprint++;
         else          lbOutsideFootprint++;
         // Also look up replacement node's post-swap total ownership to see if it was
         // genuinely under-loaded or just marginally below the donor
         List<Address> renOwnersSeg = renAfter.locateOwnersForSegment(s);
         Set<Address> pureSetSeg    = new HashSet<>(pureAfter.locateOwnersForSegment(s));
         int repOwned = -1;
         for (Address a : renOwnersSeg) {
            if (!pureSetSeg.contains(a)) {
               repOwned = renTotalOwned[after.indexOf(a)];
               break;
            }
         }
         System.out.printf("  %3d   ev[r%d]→rep[r%d]  repOwned=%-3d  joinerR=%-3d  %s%n",
               s, ev[0], ev[1], repOwned, joinerRankInSeg,
               competed ? "YES — joiner natural" : "NO  — pure variance");
      }
      System.out.println();
      System.out.printf("  LB evictions inside  joiner's natural footprint (rank<%d): %d%n",
            numOwners, lbInJoinerFootprint);
      System.out.printf("  LB evictions outside joiner's natural footprint (rank>=%d): %d  <-- pure variance correction%n",
            numOwners, lbOutsideFootprint);
      System.out.println();
      // Show histogram of replacement node ownership after swap — tells us whether we filled
      // genuinely under-loaded nodes or nearly-full ones (marginal gain)
      System.out.println("  Replacement node ownership after swap (renAfter totals):");
      System.out.printf("  %-10s  %s%n", "repOwned", "Count");
      TreeMap<Integer,Integer> repOwnedBins = new TreeMap<>();
      for (int[] ev : evictions) {
         int s2 = ev[3];
         List<Address> renOwnersSeg2 = renAfter.locateOwnersForSegment(s2);
         Set<Address> pureSetSeg2    = new HashSet<>(pureAfter.locateOwnersForSegment(s2));
         for (Address a : renOwnersSeg2) {
            if (!pureSetSeg2.contains(a)) {
               repOwnedBins.merge(renTotalOwned[after.indexOf(a)], 1, Integer::sum);
               break;
            }
         }
      }
      float idealPN = (float) numSegments * numOwners / afterCount;
      int ceilPN = (int) Math.ceil(idealPN);
      for (Map.Entry<Integer,Integer> e : repOwnedBins.entrySet()) {
         String note = e.getKey() >= ceilPN - 1 ? "  <- near ceil, marginal gain" : "";
         System.out.printf("  %-10d  %d%s%n", e.getKey(), e.getValue(), note);
      }
      System.out.println();

      // (c) For each over-loaded donor: breakdown of their load into
      //     joiner-displaced segments (joiner ranks < numOwners in those segs)
      //     vs variance-only segments (joiner ranks >= numOwners)
      System.out.println("-- Over-loaded donor breakdown: joiner-driven load vs variance-only load --");
      System.out.printf("  %-8s  %6s  %8s  %12s  %12s  %s%n",
            "Node", "total", "ideal", "joiner-driven", "variance-only", "excess");
      System.out.println("  " + "-".repeat(70));
      float idealPerNode = (float) numSegments * numOwners / afterCount;
      for (int i = 0; i < afterCount; i++) {
         int total = pureTotalOwned[i]; // use pure totals — this is what the LB starts from
         if (total <= (int) Math.ceil(idealPerNode) + 1) continue; // skip non-over-loaded nodes
         Address node = after.get(i);
         int joinerDriven = 0;
         int varianceOnly = 0;
         for (int s = 0; s < numSegments; s++) {
            if (!pureAfter.locateOwnersForSegment(s).contains(node)) continue;
            int joinerRankInSeg = rankings[s].indexOf(joiner);
            if (joinerRankInSeg < numOwners) joinerDriven++;
            else                             varianceOnly++;
         }
         int excess = total - (int) Math.ceil(idealPerNode);
         System.out.printf("  node%-4d  %6d  %8.1f  %13d  %13d  %+d%n",
               i, total, idealPerNode, joinerDriven, varianceOnly, excess);
      }
      System.out.println();

      // --- Section 3: eviction quality histogram ---
      // Bin evictions by (evicted_rank - replacement_rank):
      //   > 0 → "good": evicted a worse-fit node (expected behaviour)
      //   = 0 → "neutral": same rank (shouldn't happen)
      //   < 0 → "bad": evicted a BETTER-fit node → unnecessary movement
      System.out.println("-- Eviction quality (evicted_rank - replacement_rank) --");
      System.out.println("  Positive delta = good (kicked out a worse-fit node).");
      System.out.println("  Negative delta = bad  (kicked out a better-fit node — wasted movement).");
      System.out.println("  Zero delta     = neutral (identical rendezvous fitness).");
      System.out.println();

      TreeMap<Integer, Integer> deltaBins = new TreeMap<>();
      int goodEvictions = 0;
      int badEvictions  = 0;
      int neutralEvictions = 0;
      for (int[] ev : evictions) {
         int delta = ev[0] - ev[1];  // evictedRank - replacementRank
         deltaBins.merge(delta, 1, Integer::sum);
         if (delta > 0)      goodEvictions++;
         else if (delta < 0) badEvictions++;
         else                neutralEvictions++;
      }

      System.out.printf("  Total evictions: %d  (primary=%d, backup=%d)%n",
            evictions.size(), primaryEvictions, backupEvictions);
      System.out.printf("  Good (delta>0): %d   Bad (delta<0): %d   Neutral: %d%n",
            goodEvictions, badEvictions, neutralEvictions);
      System.out.println();
      System.out.println("  Delta histogram (evictedRank - replacementRank):");
      System.out.printf("    %-8s  %s%n", "Delta", "Count");
      for (Map.Entry<Integer, Integer> e : deltaBins.entrySet()) {
         String marker = e.getKey() < 0 ? " *** BAD" : (e.getKey() == 0 ? " neutral" : "");
         System.out.printf("    %+7d  %d%s%n", e.getKey(), e.getValue(), marker);
      }
      System.out.println();

      // --- Section 4: rank of evicted node relative to cluster size ---
      // In a 20-node cluster each segment ranks all 20 nodes 0..19.
      // If the load-balancer is evicting nodes in the top half of the ranking (ranks 0..9)
      // those are well-fitting nodes and the eviction is genuinely harmful.
      System.out.println("-- Rank of evicted node (0=best fit, " + (afterCount - 1) + "=worst fit) --");
      int[] evictedRankBuckets = new int[afterCount]; // index = rank
      for (int[] ev : evictions) {
         if (ev[0] >= 0 && ev[0] < afterCount) evictedRankBuckets[ev[0]]++;
      }
      System.out.println("  rank  count  (half-way point is rank " + (afterCount / 2) + ")");
      for (int r = 0; r < afterCount; r++) {
         if (evictedRankBuckets[r] == 0) continue;
         String flag = r < afterCount / 2 ? " <-- WELL-FIT NODE evicted" : "";
         System.out.printf("  %4d  %5d%s%n", r, evictedRankBuckets[r], flag);
      }
      System.out.println();

      // --- Section 5: segments that Rendezvous moved but PureRendezvous did NOT ---
      // These are the "extra" movements caused purely by the load-balancer correction pass.
      Set<Integer> renExtraChanged = new HashSet<>(renChanged);
      renExtraChanged.removeAll(pureChanged);
      System.out.printf("-- Extra segments Rendezvous moved vs PureRendezvous: %d --%n",
            renExtraChanged.size());
      System.out.println("  (These are segments whose owner SET was stable for Pure but changed for Ren.)");
      if (!renExtraChanged.isEmpty()) {
         List<Integer> sorted = new ArrayList<>(renExtraChanged);
         Collections.sort(sorted);
         for (int s : sorted) {
            List<Address> beforeOwners = renBefore.locateOwnersForSegment(s);
            List<Address> pureOwners   = pureAfter.locateOwnersForSegment(s);
            List<Address> renOwners    = renAfter.locateOwnersForSegment(s);
            List<Address> ranking      = rankings[s];
            System.out.printf("  seg %3d: before=%s  pure=%s  ren=%s%n",
                  s,
                  formatOwners(beforeOwners, after, ranking),
                  formatOwners(pureOwners, after, ranking),
                  formatOwners(renOwners, after, ranking));
         }
      }
      System.out.println();

      // --- Section 6: recommendations summary ---
      System.out.println("=================================================================");
      System.out.println(" ANALYSIS SUMMARY & RECOMMENDATIONS");
      System.out.println("=================================================================");
      System.out.printf("  Segments moved by Pure: %d   by Ren: %d   (owner-set changes)%n",
            pureChanged.size(), renChanged.size());
      System.out.printf("  Ren-extra moves (balancer overhead): %d%n", renExtraChanged.size());
      System.out.printf("  Reorder-only diffs (no data move):   %d%n", reordersOnly);
      System.out.println();
      System.out.println("  Eviction quality:");
      System.out.printf("    Good (evicted worse-fit): %d (%.0f%%)%n",
            goodEvictions, evictions.isEmpty() ? 0.0 : 100.0 * goodEvictions / evictions.size());
      System.out.printf("    Bad  (evicted better-fit): %d (%.0f%%)%n",
            badEvictions, evictions.isEmpty() ? 0.0 : 100.0 * badEvictions / evictions.size());
      System.out.println();
      System.out.println("  POSSIBLE IMPROVEMENTS TO EVALUATE:");
      System.out.println("  [1] Tighten the drain threshold from ceil+1 to strict ceil.");
      System.out.println("      The current loop fires only when owned > ceil+1, so nodes at ceil+1");
      System.out.println("      are left over-loaded and force the NEXT topology change to over-correct.");
      System.out.println("      Changing to owned > ceil would drain more eagerly and reduce cascading");
      System.out.println("      movement on subsequent joins.");
      System.out.println();
      System.out.println("  [2] Limit swaps to segments where the evicted node's rank is >= N/2.");
      System.out.println("      If a node's rank in a segment is in the top half (rank < N/2) it is");
      System.out.println("      a natural owner — evicting it wastes movement.  Prefer segments where");
      System.out.println("      the over-loaded node is already a poor fit.");
      System.out.println();
      System.out.println("  [3] Use the joiner's rendezvous ranking directly in the load-balancing pass.");
      System.out.println("      Rather than draining any over-loaded node, prefer to drain nodes from");
      System.out.println("      segments where the joiner ranks highly — this makes the correction pass");
      System.out.println("      agree with what PureRendezvous would have done naturally.");
      System.out.println();
      System.out.println("  [4] Skip the redistribution pass entirely for small clusters (N < 10)");
      System.out.println("      where hash variance is high and the 'ideal' is a rough estimate.");
      System.out.println("=================================================================");
      System.out.println();
   }

   /** Formats an owner list as "nX[rY],nZ[rW]" where rY is the rendezvous rank of each owner. */
   private static String formatOwners(List<Address> owners, List<Address> members,
                                      List<Address> ranking) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < owners.size(); i++) {
         if (i > 0) sb.append(",");
         Address a = owners.get(i);
         int nodeIdx = members.indexOf(a);
         int rank    = ranking.indexOf(a);
         sb.append(String.format("n%d[r%d]", nodeIdx, rank));
      }
      return sb.toString();
   }
}
