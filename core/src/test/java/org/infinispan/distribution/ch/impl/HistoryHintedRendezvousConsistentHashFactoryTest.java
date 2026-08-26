package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.RendezvousSegmentMovementTest.makeNodes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tests specific to {@link HistoryHintedRendezvousConsistentHashFactory}.
 *
 * <p>Verifies the stability and correctness properties of the history-hinted rebalance:</p>
 * <ul>
 *   <li>After a shared topology change, two caches with different prior histories converge toward
 *       fewer differing segments (tested by {@code testConvergesAfterSharedTopologyChanges}).</li>
 *   <li>Two {@code create()} calls on the same member set produce identical assignments
 *       (tested by {@code testFreshCreateIsDeterministic}).</li>
 *   <li>{@code rebalance()} returns the exact same instance when passed a fully-balanced CH
 *       that did not come from {@code updateMembers()} — i.e. the early-exit fires and no
 *       spurious movement is triggered (tested by {@code testRebalanceIsIdempotentOnBalancedCH}).</li>
 * </ul>
 */
@Test(groups = "unit", testName = "distribution.ch.HistoryHintedRendezvousConsistentHashFactoryTest")
public class HistoryHintedRendezvousConsistentHashFactoryTest extends AbstractInfinispanTest {

   private static final HistoryHintedRendezvousConsistentHashFactory CHF =
         HistoryHintedRendezvousConsistentHashFactory.getInstance();

   /**
    * Two caches diverge by seeing different temporary nodes join and leave, then observe 5
    * shared topology passes: join, leave, join, leave, bulk-join (+3). The bulk-join at pass 5
    * creates enough rebalancing pressure to flush the residual history divergence.
    *
    * <p>Asserts that the gap after the 5 shared passes is strictly less than the initial gap.</p>
    */
   public void testConvergesAfterSharedTopologyChanges() {
      int numSegments = 256;
      int numOwners = 2;

      List<Address> sharedBase = makeNodes(6);
      DefaultConsistentHash sharedStart = CHF.create(numOwners, numSegments, sharedBase, null);

      // Cache A: a temporary node joins then leaves
      List<Address> membersA = new ArrayList<>(sharedBase);
      Address extraA = Address.random("extraA");
      membersA.add(extraA);
      DefaultConsistentHash chA = CHF.rebalance(CHF.updateMembers(sharedStart, membersA, null));
      membersA.remove(extraA);
      chA = CHF.rebalance(CHF.updateMembers(chA, membersA, null));

      // Cache B: a *different* temporary node joins then leaves
      List<Address> membersB = new ArrayList<>(sharedBase);
      Address extraB = Address.random("extraB");
      membersB.add(extraB);
      DefaultConsistentHash chB = CHF.rebalance(CHF.updateMembers(sharedStart, membersB, null));
      membersB.remove(extraB);
      chB = CHF.rebalance(CHF.updateMembers(chB, membersB, null));

      assertEquals(chA.getMembers(), chB.getMembers(),
            "Both caches must be on the same member list before the convergence passes");

      int initialGap = countSegmentDiff(chA, chB, numSegments);

      // 5 shared passes on the same base member list
      List<Address> members = new ArrayList<>(sharedBase);
      int[] gaps = new int[5];

      // Pass 1: join
      Address p1 = Address.random();
      members.add(p1);
      chA = CHF.rebalance(CHF.updateMembers(chA, members, null));
      chB = CHF.rebalance(CHF.updateMembers(chB, members, null));
      gaps[0] = countSegmentDiff(chA, chB, numSegments);

      // Pass 2: leave
      members.remove(p1);
      chA = CHF.rebalance(CHF.updateMembers(chA, members, null));
      chB = CHF.rebalance(CHF.updateMembers(chB, members, null));
      gaps[1] = countSegmentDiff(chA, chB, numSegments);

      // Pass 3: join
      Address p3 = Address.random();
      members.add(p3);
      chA = CHF.rebalance(CHF.updateMembers(chA, members, null));
      chB = CHF.rebalance(CHF.updateMembers(chB, members, null));
      gaps[2] = countSegmentDiff(chA, chB, numSegments);

      // Pass 4: leave
      members.remove(p3);
      chA = CHF.rebalance(CHF.updateMembers(chA, members, null));
      chB = CHF.rebalance(CHF.updateMembers(chB, members, null));
      gaps[3] = countSegmentDiff(chA, chB, numSegments);

      // Pass 5: bulk join (+3) to create rebalancing pressure that flushes divergence
      for (int i = 0; i < 3; i++) {
         members.add(Address.random());
      }
      chA = CHF.rebalance(CHF.updateMembers(chA, members, null));
      chB = CHF.rebalance(CHF.updateMembers(chB, members, null));
      gaps[4] = countSegmentDiff(chA, chB, numSegments);

      assertTrue(gaps[4] < initialGap || initialGap == 0,
            "HistoryHinted gap must shrink (or start at 0) after 5 shared topology passes"
                  + " (initial=" + initialGap
                  + ", pass1=" + gaps[0] + ", pass2=" + gaps[1]
                  + ", pass3=" + gaps[2] + ", pass4=" + gaps[3]
                  + ", pass5=" + gaps[4] + ")");
   }

   /**
    * Verifies that {@code rebalance()} returns the exact same {@link DefaultConsistentHash}
    * instance (reference equality) when the input is already fully balanced — every segment has
    * {@code actualNumOwners} owners and every member owns at least one segment.
    *
    * <p>This exercises the early-exit path that prevents {@code waitForNoRebalance} from
    * looping indefinitely: once the cluster has converged, calling {@code rebalance()} on the
    * resulting CH must be a no-op that returns {@code baseCH} unchanged.</p>
    *
    * <p>The test builds a balanced CH via a full join cycle (create → updateMembers → rebalance),
    * confirms the result is indeed balanced, then calls {@code rebalance()} again on it and
    * asserts reference equality ({@code assertSame}) rather than value equality, because the
    * contract requires returning the exact {@code baseCH} instance.</p>
    */
   public void testRebalanceIsIdempotentOnBalancedCH() {
      int numSegments = 256;
      int numOwners = 2;

      // Build a stable, fully-balanced CH through a normal join cycle
      List<Address> before = makeNodes(8);
      DefaultConsistentHash initial = CHF.create(numOwners, numSegments, before, null);

      List<Address> after = new ArrayList<>(before);
      after.add(Address.random("joiner"));
      DefaultConsistentHash balanced = CHF.rebalance(CHF.updateMembers(initial, after, null));

      // Confirm it is genuinely balanced: every segment must have exactly numOwners owners
      // and every member must own at least one segment
      int actualNumOwners = Math.min(numOwners, after.size());
      for (int s = 0; s < numSegments; s++) {
         assertEquals(actualNumOwners, balanced.locateOwnersForSegment(s).size(),
               "Segment " + s + " must have exactly " + actualNumOwners + " owners after rebalance");
      }
      for (Address member : after) {
         assertTrue(!balanced.getSegmentsForOwner(member).isEmpty(),
               "Member " + member + " must own at least one segment after rebalance");
      }

      // Now call rebalance() again — must return the exact same instance
      DefaultConsistentHash second = CHF.rebalance(balanced);
      assertSame(balanced, second,
            "rebalance() must return baseCH unchanged when the CH is already fully balanced");
   }

   /**
    * Verifies that two {@code create()} calls on the same member list produce identical
    * assignments — the factory is deterministic when there is no prior history.
    */
   public void testFreshCreateIsDeterministic() {
      List<Address> members = makeNodes(8);
      DefaultConsistentHash ch1 = CHF.create(2, 256, members, null);
      DefaultConsistentHash ch2 = CHF.create(2, 256, members, null);
      assertEquals(ch1, ch2,
            "HistoryHinted create() must be deterministic for the same member list");
   }

   /**
    * Verifies that with 2 nodes, 2 segments and 2 owners the factory produces exactly one
    * primary-owner segment per node after a fresh rebalance.
    *
    * <p>With 2 members and 2 segments the ideal primary load is exactly 1 per node. The
    * history-hinted factory must honour that and assign each node as the primary for precisely
    * one segment, regardless of which node the rendezvous hash ranks first.</p>
    */
   public void testTwoNodesTwoSegmentsOnePrimaryEach() {
      Address A = Address.random("A");
      Address B = Address.random("B");
      List<Address> members = Arrays.asList(A, B);

      DefaultConsistentHash ch = CHF.rebalance(CHF.create(2, 2, members, null));

      Set<Integer> primaryA = ch.getPrimarySegmentsForOwner(A);
      Set<Integer> primaryB = ch.getPrimarySegmentsForOwner(B);

      assertEquals(1, primaryA.size(),
            "Node A must be primary for exactly 1 segment, got: " + primaryA);
      assertEquals(1, primaryB.size(),
            "Node B must be primary for exactly 1 segment, got: " + primaryB);

      // The two primary sets must be disjoint and cover all segments
      assertFalse(primaryA.stream().anyMatch(primaryB::contains), "A and B must not share a primary segment");
      assertEquals(2, primaryA.size() + primaryB.size(),
            "All 2 segments must have a primary owner");
   }

   // ---- Helpers ----

   /** Counts segments where the two CHs have a different ordered owner list. */
   private static int countSegmentDiff(DefaultConsistentHash a, DefaultConsistentHash b,
                                        int numSegments) {
      int diff = 0;
      for (int s = 0; s < numSegments; s++) {
         if (!a.locateOwnersForSegment(s).equals(b.locateOwnersForSegment(s))) {
            diff++;
         }
      }
      return diff;
   }
}
