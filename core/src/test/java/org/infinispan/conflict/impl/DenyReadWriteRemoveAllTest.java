package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.partitionhandling.AvailabilityMode.DEGRADED_MODE;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashSet;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.DenyReadWriteRemoveAllTest")
public class DenyReadWriteRemoveAllTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      /*
       * AD => All partitions are degraded during split
       * PD => Only the minority partition is degraded during split
       * N => total number of nodes
       */
      return new Object[] {
            new DenyReadWriteRemoveAllTest(REPL_SYNC, "PD-5N", new int[]{0,1,2}, new int[]{3,4}),
            new DenyReadWriteRemoveAllTest(REPL_SYNC, "AD-4N", DEGRADED_MODE, new int[]{0,1}, new int[]{2,3}),
            new DenyReadWriteRemoveAllTest(REPL_SYNC, "PD-4N", new int[]{0,1,2}, new int[]{3}),
            new DenyReadWriteRemoveAllTest(REPL_SYNC, "PD-3N", new int[]{0,1}, new int[]{2}),
            new DenyReadWriteRemoveAllTest(REPL_SYNC, "AD-2N", DEGRADED_MODE, new int[]{0}, new int[]{1}),

            new DenyReadWriteRemoveAllTest(DIST_SYNC, "AD-5N", DEGRADED_MODE, new int[]{0,1,2}, new int[]{3,4}),
            new DenyReadWriteRemoveAllTest(DIST_SYNC, "AD-4N", DEGRADED_MODE, new int[]{0,1}, new int[]{2,3}),
            new DenyReadWriteRemoveAllTest(DIST_SYNC, "PD-4N", new int[]{0,1,2}, new int[]{3}),
            new DenyReadWriteRemoveAllTest(DIST_SYNC, "PD-3N", new int[]{0,1}, new int[]{2}),
            new DenyReadWriteRemoveAllTest(DIST_SYNC, "AD-2N", DEGRADED_MODE, new int[]{0}, new int[]{1}),
      };
   }

   public DenyReadWriteRemoveAllTest(){}

   public DenyReadWriteRemoveAllTest(CacheMode cacheMode, String description, int[] partition1, int[] partition2) {
      this(cacheMode, description, null, partition1, partition2);
   }

   public DenyReadWriteRemoveAllTest(CacheMode cacheMode, String description, AvailabilityMode availabilityMode,
                                     int[] partition1, int[] partition2) {
      super(cacheMode, description, availabilityMode, partition1, partition2);
      this.mergePolicy = MergePolicy.REMOVE_ALL;
      this.partitionHandling = PartitionHandling.DENY_READ_WRITES;
      this.valueAfterMerge = null;
   }

   @Override
   protected void beforeSplit() {
      // Put values locally before the split as an AvaililibityException will be thrown during the split
      conflictKey = new MagicKey(cache(p0.node(0)), cache(p1.node(0)));
      cache(p0.node(0)).put(conflictKey, "V1");
      cache(p1.node(0)).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put(conflictKey, "V2");
   }

   @Override
   protected void splitCluster() {
      List<Address> allMembers = advancedCache(0).getRpcManager().getMembers();
      for (int i = 0; i < numMembersInCluster; i++)
         assertEquals(new HashSet<>(partitionHandlingManager(i).getLastStableTopology().getMembers()), new HashSet<>(allMembers));

      if (p0.getExpectedMode() == DEGRADED_MODE) {
         eventually(() -> {
            for (int i = 0; i < numMembersInCluster; i++)
               if (partitionHandlingManager(i).getAvailabilityMode() != AVAILABLE)
                  return false;
            return true;
         });
      }

      splitCluster(p0.getNodes(), p1.getNodes());

      if (p0.getExpectedMode() != DEGRADED_MODE) {
         TestingUtil.waitForNoRebalance(getPartitionCaches(p0));
      } else {
         for (int i = 0; i < numMembersInCluster; i++)
            assertEquals(new HashSet<>(partitionHandlingManager(i).getLastStableTopology().getMembers()), new HashSet<>(allMembers));

         partition(0).assertDegradedMode();
         partition(0).assertKeyNotAvailableForRead(conflictKey);
      }

      partition(1).assertDegradedMode();
      partition(1).assertKeyNotAvailableForRead(conflictKey);
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) {
   }

   @Override
   protected void afterConflictResolutionAndMerge() {
      partition(0).assertAvailabilityMode(AVAILABLE);
      super.afterConflictResolutionAndMerge();
      assertNull(cache(p0.node(0)).get(conflictKey));
   }
}
