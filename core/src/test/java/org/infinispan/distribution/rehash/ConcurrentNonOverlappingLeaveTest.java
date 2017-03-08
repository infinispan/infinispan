package org.infinispan.distribution.rehash;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentNonOverlappingLeaveTest")
public class ConcurrentNonOverlappingLeaveTest extends RehashLeaveTestBase {
   Address l1, l2;
   // since two nodes are leaving, we allow some entries to be lost
   private Set<Integer> lostSegments = new HashSet<>();

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      if (lostSegments.contains(getCacheTopology(c1).getSegment(key)))
         return;

      super.assertOwnershipAndNonOwnership(key, allowL1);
   }

   @Override
   protected void assertOnAllCaches(Object key, String value) {
      if (lostSegments.contains(getCacheTopology(c1).getSegment(key)))
      return;

      super.assertOnAllCaches(key, value);
   }

   void performRehashEvent(boolean offline) {
      l1 = addressOf(c2);
      l2 = addressOf(c4);
      List<Address> killedNodes = Arrays.asList(l1, l2);

      CacheContainer cm2 = c2.getCacheManager();
      CacheContainer cm4 = c4.getCacheManager();

      Set<Integer> overlappingSegments = new HashSet<>();
      ConsistentHash ch = getCacheTopology(c1).getWriteConsistentHash();
      for (int segment = 0; segment < ch.getNumSegments(); segment++) {
         List<Address> owners = ch.locateOwnersForSegment(segment);
         if (owners.containsAll(killedNodes)) {
            overlappingSegments.add(segment);
         }
      }
      lostSegments = overlappingSegments;
      log.tracef("These segments will be lost after killing nodes %s: %s", killedNodes, lostSegments);

      cacheManagers.removeAll(Arrays.asList(cm2, cm4));
      caches.removeAll(Arrays.asList(c2, c4));

      TestingUtil.killCacheManagers(cm2, cm4);
   }
}
