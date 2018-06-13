package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.IndependentClustersMergeTest")
public class IndependentClustersMergeTest extends BasePartitionHandlingTest {

   private static final String MERGE_RESULT = "merge-result";

   @Override
   public Object[] factory() {
      return new Object[] {
            new IndependentClustersMergeTest().partitionHandling(PartitionHandling.ALLOW_READS),
            new IndependentClustersMergeTest().partitionHandling(PartitionHandling.DENY_READ_WRITES)
      };
   }

   public IndependentClustersMergeTest() {
      this.numMembersInCluster = 2;
      this.mergePolicy = (preferredEntry, otherEntries) -> {
         CacheEntry<String, String> entry = preferredEntry != null ? preferredEntry : otherEntries.get(0);
         entry.setValue(MERGE_RESULT);
         return entry;
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering()
            .cacheMode(cacheMode)
            .partitionHandling().whenSplit(partitionHandling).mergePolicy(mergePolicy)
            .hash().numOwners(numberOfOwners);

      for (int i = 0; i < numMembersInCluster; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(dcc, new TransportFlags().withFD(true).withMerge(true));
         Cache cache = cm.getCache();
         disableDiscoveryProtocol(channel(cache));
      }
   }

   public void testConflictResolutionCalled() {
      Cache c0 = cache(0);
      Cache c1 = cache(1);
      assertEquals(1, topologySize(c0));
      assertEquals(1, topologySize(c1));
      c0.put(1, 1);
      c1.put(1, 2);
      enableDiscoveryProtocol(channel(c0));
      enableDiscoveryProtocol(channel(c1));
      TestingUtil.waitForNoRebalance(c0, c1);
      assertEquals(MERGE_RESULT, c0.get(1));
   }

   private int topologySize(Cache cache) {
      return cache.getAdvancedCache().getDistributionManager().getCacheTopology().getMembers().size();
   }
}
