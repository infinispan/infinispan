package org.infinispan.conflict.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * ISPN-8925 This test creates several caches and then initiates a split-brain followed by a merge. The purpose of this
 * test is to ensure that when many caches exist, it's still possible for conflict resolution and the rebalance to proceed.
 * Previously the executor in the ClusterTopologyManagerImpl would be blocked indefinitely if the number of caches was
 * >= ProcessorInfo.availableProcessors() / 2 + 1
 *
 * @author Ryan Emerson
 */
@Test(groups = "functional", testName = "conflict.impl.MultipleCachesDuringConflictResolutionTest")
public class MultipleCachesDuringConflictResolutionTest extends BasePartitionHandlingTest {

   // Does not include the org.infinispan.CONFIG and ___defaultCache, so total caches = numberOfCaches + 2
   private final int numberOfCaches = 10;
   private final int numMembersInCluster;
   private final PartitionDescriptor p0;
   private final PartitionDescriptor p1;

   public MultipleCachesDuringConflictResolutionTest() {
      this.p0 = new PartitionDescriptor(0);
      this.p1 = new PartitionDescriptor(1);
      this.numMembersInCluster = 2;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering().cacheMode(cacheMode)
         .partitionHandling().whenSplit(PartitionHandling.ALLOW_READ_WRITES).mergePolicy(MergePolicy.PREFERRED_ALWAYS);
      String[] cacheNames = getCacheNames();
      // Create a default cache because waitForPartitionToForm() needs it
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gc.defaultCacheName(cacheNames[0]);
      createClusteredCaches(numMembersInCluster, gc, dcc, false, new TransportFlags().withFD(true).withMerge(true),
                            cacheNames);

      waitForClusterToForm(cacheNames);
   }

   private String[] getCacheNames() {
      String[] cacheNames = new String[numberOfCaches];
      for (int i = 0; i < numberOfCaches; i++)
         cacheNames[i] = "cache" + i;
      return cacheNames;
   }

   public void testPartitionMergePolicy() {
      TestingUtil.waitForNoRebalanceAcrossManagers(managers());

      log.tracef("split test");
      splitCluster(p0.getNodes(), p1.getNodes());
      TestingUtil.waitForNoRebalanceAcrossManagers(manager(0));
      TestingUtil.waitForNoRebalanceAcrossManagers(manager(1));

      log.tracef("performMerge");
      partition(0).merge(partition(1));
      TestingUtil.waitForNoRebalanceAcrossManagers(managers());
   }
}
