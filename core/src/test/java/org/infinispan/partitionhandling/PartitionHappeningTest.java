package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.PartitionHappeningTest")
@InCacheMode({CacheMode.DIST_SYNC })
public class PartitionHappeningTest extends BasePartitionHandlingTest {

   public PartitionHappeningTest() {
      partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   public void testPartitionHappening() throws Throwable {

      final List<ViewChangedHandler> listeners = new ArrayList<>();
      for (int i = 0; i < caches().size(); i++) {
         ViewChangedHandler listener = new ViewChangedHandler();
         cache(i).getCacheManager().addListener(listener);
         listeners.add(listener);
      }

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      eventually(() -> {
         for (ViewChangedHandler l : listeners)
            if (!l.isNotified()) return false;
         return true;
      });

      eventuallyEquals(2, () -> advancedCache(0).getRpcManager().getTransport().getMembers().size());
      eventually(() -> clusterAndChFormed(0, 2));
      eventually(() -> clusterAndChFormed(1, 2));
      eventually(() -> clusterAndChFormed(2, 2));
      eventually(() -> clusterAndChFormed(3, 2));


      cache(0).put("k", "v1");
      cache(2).put("k", "v2");

      assertEquals(cache(0).get("k"), "v1");
      assertEquals(cache(1).get("k"), "v1");
      assertEquals(cache(2).get("k"), "v2");
      assertEquals(cache(3).get("k"), "v2");

      partition(0).merge(partition(1));
      assertTrue(clusterAndChFormed(0, 4));
      assertTrue(clusterAndChFormed(1, 4));
      assertTrue(clusterAndChFormed(2, 4));
      assertTrue(clusterAndChFormed(3, 4));
   }

   public boolean clusterAndChFormed(int cacheIndex, int memberCount) {
      return advancedCache(cacheIndex).getRpcManager().getTransport().getMembers().size() == memberCount &&
            advancedCache(cacheIndex).getDistributionManager().getWriteConsistentHash().getMembers().size() == memberCount;
   }

}
