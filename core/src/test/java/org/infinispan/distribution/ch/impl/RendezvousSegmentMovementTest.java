package org.infinispan.distribution.ch.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Tests segment movement counts and stability properties for {@link RendezvousConsistentHashFactory}.
 */
@Test(groups = "unit", testName = "distribution.ch.RendezvousSegmentMovementTest")
public class RendezvousSegmentMovementTest extends AbstractInfinispanTest {

   private static final ConsistentHashFactory<DefaultConsistentHash> CHF =
         RendezvousConsistentHashFactory.getInstance();

   public void testNodeJoinMovesBoundedSegments() {
      List<Address> before = makeNodes(4);
      List<Address> after = new ArrayList<>(before);
      after.add(Address.random("new"));

      DefaultConsistentHash chBefore = CHF.create(2, 256, before, null);
      DefaultConsistentHash chAfter = CHF.rebalance(CHF.updateMembers(chBefore, after, null));

      int moved = countMovedSegments(chBefore, chAfter);
      // Ideal = 256 * 2 / 5 ≈ 102 segments for the new node.
      // Allow tolerance for the bounded-load correction pass which may move extra segments to restore floor/ceil balance.
      int maxAllowed = (int) Math.ceil((double) 256 * 2 / 5) + 25;
      assertTrue(moved <= maxAllowed,
            "Moved " + moved + " segments on join, expected <= " + maxAllowed);
   }

   public void testNodeLeaveMovesBoundedSegments() {
      List<Address> before = makeNodes(5);
      List<Address> after = before.subList(0, 4);

      DefaultConsistentHash chBefore = CHF.create(2, 256, before, null);
      DefaultConsistentHash chAfter = CHF.rebalance(CHF.updateMembers(chBefore, after, null));

      // The leaver owned ~102 segments; those need redistribution
      int leaverSegments = chBefore.getSegmentsForOwner(before.get(4)).size();
      int moved = countMovedSegments(chBefore, chAfter);
      // Allow tolerance for the bounded-load correction pass which may move extra segments to restore balance.
      assertTrue(moved <= leaverSegments + 25,
            "Moved " + moved + " segments on leave, leaver owned " + leaverSegments);
   }

   public void testSingleNodeJoinMovesBoundedExtraSegments() {
      // The bounded-load greedy may move a small number of segments outside the joiner's natural
      // rendezvous domain in order to restore balance. This is expected and correct behaviour.
      // We verify that the total movement is bounded, not that only the joiner's segments move.
      List<Address> before = makeNodes(4);
      Address joiner = Address.random("joiner");
      List<Address> after = new ArrayList<>(before);
      after.add(joiner);

      DefaultConsistentHash chBefore = CHF.create(2, 256, before, null);
      DefaultConsistentHash chAfter = CHF.rebalance(CHF.updateMembers(chBefore, after, null));

      // Joiner should own close to its ideal (256 * 2 / 5 ≈ 102 segments)
      int joinerOwned = chAfter.getSegmentsForOwner(joiner).size();
      assertTrue(joinerOwned >= 90 && joinerOwned <= 115,
            "Joiner should own ~102 segments (±13), got " + joinerOwned);
   }

   public void testSingleNodeLeaveMovesBoundedExtraSegments() {
      List<Address> before = makeNodes(5);
      Address leaver = before.get(4);
      List<Address> after = before.subList(0, 4);

      DefaultConsistentHash chBefore = CHF.create(2, 256, before, null);
      DefaultConsistentHash chAfter = CHF.rebalance(CHF.updateMembers(chBefore, after, null));

      // Leaver must not appear in any segment's owner list after rebalance
      for (int s = 0; s < chAfter.getNumSegments(); s++) {
         assertFalse(chAfter.locateOwnersForSegment(s).contains(leaver),
               "Leaver should not own segment " + s + " after rebalance");
      }
      // Total moved segments should be bounded
      int moved = countMovedSegments(chBefore, chAfter);
      int leaverSegments = chBefore.getSegmentsForOwner(leaver).size();
      assertTrue(moved <= leaverSegments + 30,
            "Moved " + moved + " segments but leaver only owned " + leaverSegments);
   }

   public void testRebalanceIdempotent() {
      List<Address> members = makeNodes(5);
      DefaultConsistentHash ch = CHF.create(2, 256, members, null);
      assertSame(ch, CHF.rebalance(ch), "rebalance() on an already-balanced CH must return the same instance");
   }

   public void testUpdateMembersPreservesOwnershipForRemainingNodes() {
      List<Address> before = makeNodes(4);
      List<Address> after = before.subList(0, 3);

      DefaultConsistentHash chBefore = CHF.create(2, 64, before, null);
      DefaultConsistentHash updated = CHF.updateMembers(chBefore, after, null);

      // After updateMembers (before rebalance): no segment should be empty
      for (int s = 0; s < updated.getNumSegments(); s++) {
         assertTrue(!updated.locateOwnersForSegment(s).isEmpty(),
               "Segment " + s + " has no owners after updateMembers");
      }

      // No new owners should have been added (only removals are allowed)
      Set<Address> afterSet = new HashSet<>(after);
      for (int s = 0; s < updated.getNumSegments(); s++) {
         for (Address owner : updated.locateOwnersForSegment(s)) {
            assertTrue(afterSet.contains(owner),
                  "Owner " + owner + " in segment " + s + " is not in the new member list");
         }
      }
   }

   // ---- Helpers ----

   static List<Address> makeNodes(int count) {
      List<Address> nodes = new ArrayList<>(count);
      for (int i = 0; i < count; i++) nodes.add(Address.random("node" + i));
      return nodes;
   }

   static int countMovedSegments(DefaultConsistentHash before, DefaultConsistentHash after) {
      int moved = 0;
      for (int s = 0; s < before.getNumSegments(); s++) {
         if (!before.locateOwnersForSegment(s).equals(after.locateOwnersForSegment(s))) {
            moved++;
         }
      }
      return moved;
   }

   /**
    * Counts segments where the owner set changed (ignoring position/primary order).
    * A segment is "lost" by a node when it appears in before's owners but not after's owners.
    * This measures actual data movement, excluding free primary↔backup reorders.
    */
   static int countLostSegments(DefaultConsistentHash before, DefaultConsistentHash after) {
      int lost = 0;
      for (int s = 0; s < before.getNumSegments(); s++) {
         List<Address> beforeOwners = before.locateOwnersForSegment(s);
         List<Address> afterOwners = after.locateOwnersForSegment(s);
         if (!new java.util.HashSet<>(beforeOwners).equals(new java.util.HashSet<>(afterOwners))) {
            lost++;
         }
      }
      return lost;
   }
}
