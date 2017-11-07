package org.infinispan.conflict.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashSet;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.conflict.MergePolicies;
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
            new DenyReadWriteRemoveAllTest().setPartitions("AD-5N", AvailabilityMode.DEGRADED_MODE, new int[]{0,1,2}, new int[]{3,4}),
            new DenyReadWriteRemoveAllTest().setPartitions("AD-4N", AvailabilityMode.DEGRADED_MODE, new int[]{0,1}, new int[]{2,3}),
            new DenyReadWriteRemoveAllTest().setPartitions("PD-4N", new int[]{0,1,2}, new int[]{3}),
            new DenyReadWriteRemoveAllTest().setPartitions("PD-3N", new int[]{0,1}, new int[]{2}),
      };
   }

   public DenyReadWriteRemoveAllTest() {
      super();
      this.mergePolicy = MergePolicies.REMOVE_ALL;
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

      if (p0.getExpectedMode() == AvailabilityMode.DEGRADED_MODE) {
         eventually(() -> {
            for (int i = 0; i < numMembersInCluster; i++)
               if (partitionHandlingManager(i).getAvailabilityMode() != AvailabilityMode.AVAILABLE)
                  return false;
            return true;
         });
      }

      splitCluster(p0.getNodes(), p1.getNodes());

      if (p0.getExpectedMode() != AvailabilityMode.DEGRADED_MODE) {
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
      partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);
      super.afterConflictResolutionAndMerge();
      assertNull(cache(p0.node(0)).get(conflictKey));
   }
}
