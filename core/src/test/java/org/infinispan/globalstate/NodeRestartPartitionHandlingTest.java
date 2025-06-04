package org.infinispan.globalstate;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.partitionhandling.BaseStatefulPartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.MissingMembersException;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.NodeRestartPartitionHandlingTest")
public class NodeRestartPartitionHandlingTest extends BaseStatefulPartitionHandlingTest {

   {
      partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
      numMembersInCluster = 2;
   }

   public void testRestartDuringNetworkPartition() throws Throwable {
      var addressMappings = createInitialCluster();
      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();

      for (int i = 0; i < numMembersInCluster; i++) {
         ((DefaultCacheManager) manager(i)).shutdownAllCaches();
      }

      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists
      for (int i = 0; i < numMembersInCluster; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
      cacheManagers.clear();

      createStatefulCacheManagers(false);

      // We split the cluster. This should make the caches not be able to restore.
      splitCluster(new int[]{0}, new int[]{1});
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      // We restart the cluster, completely. Caches should issue join requests during partition.
      for (int i = 0; i < numMembersInCluster; i++) {
         cache(i, CACHE_NAME);
      }

      // Assert we still partitioned.
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      // Since the cluster is partitioned, the cache didn't recovered, operations should fail.
      assertOperationsFail();

      // Merge the cluster. This should make the caches restore.
      partition(0).merge(partition(1), false);
      waitForClusterToForm(CACHE_NAME);
      assertHealthyCluster(addressMappings, oldConsistentHash);
   }

   private void assertOperationsFail() {
      for (int i = 0; i < cacheManagers.size(); i++) {
         for (int v = 0; v < DATA_SIZE; v++) {
            final Cache<Object, Object> cache = cache(i, CACHE_NAME);
            String key = String.valueOf(v);
            // Always returns null. Message about not stable yet is logged.
            Exceptions.expectException(MissingMembersException.class,
                  "ISPN000689: Recovering cache 'testCache' but there are missing members, known members \\[.*\\] of a total of 2$",
                  () -> cache.get(key));
         }
      }
   }
}
