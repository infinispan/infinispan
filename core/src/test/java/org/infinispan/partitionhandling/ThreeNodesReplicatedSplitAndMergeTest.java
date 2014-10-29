package org.infinispan.partitionhandling;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "partitionhandling.ThreeNodesReplicatedSplitAndMergeTest")
public class ThreeNodesReplicatedSplitAndMergeTest extends BasePartitionHandlingTest {

   public ThreeNodesReplicatedSplitAndMergeTest() {
      numMembersInCluster = 3;
      cacheMode = CacheMode.REPL_SYNC;
   }

   public void testSplitAndMerge0() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(0, 1), new PartitionDescriptor(2));
   }

   public void testSplitAndMerge1() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(0, 2), new PartitionDescriptor(1));
   }

   public void testSplitAndMerge2() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(1, 2), new PartitionDescriptor(0));
   }

   private void testSplitAndMerge(PartitionDescriptor p0, PartitionDescriptor p1) throws Exception {
      Object k0 = new MagicKey(cache(p0.node(0)), cache(p0.node(1)));
      cache(0).put(k0, "v0");

      Object k1 = new MagicKey(cache(p0.node(1)), cache(p1.node(0)));
      cache(1).put(k1, "v1");

      Object k2 = new MagicKey(cache(p1.node(0)), cache(p0.node(0)));
      cache(2).put(k2, "v2");


      List<Address> allMembers = advancedCache(0).getRpcManager().getMembers();
      //use set comparison as the merge view will reshuffle the order of nodes
      assertEquals(new HashSet<>(partitionHandlingManager(0).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(1).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(2).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < numMembersInCluster; i++) {
               if (partitionHandlingManager(i).getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
                  return false;
               }
            }
            return true;
         }
      });

      splitCluster(p0.getNodes(), p1.getNodes());

      TestingUtil.waitForRehashToComplete(cache(p0.node(0)), cache(p0.node(1)));
      partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);
      partition(1).assertAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
      assertEquals(partitionHandlingManager(p1.node(0)).getLastStableTopology().getMembers(), allMembers);

      //1. check key visibility in partition 1
      partition(0).assertKeyAvailableForRead(k0, "v0");
      partition(0).assertKeyAvailableForRead(k1, "v1");
      partition(0).assertKeyAvailableForRead(k2, "v2");

      //2. check key visibility in partition 2
      partition(1).assertKeysNotAvailableForRead(k0, k1, k2);

      //3. check key ownership
      assertTrue(dataContainer(p0.node(0)).containsKey(k0));
      assertTrue(dataContainer(p0.node(0)).containsKey(k1));
      assertTrue(dataContainer(p0.node(0)).containsKey(k2));

      assertTrue(dataContainer(p0.node(1)).containsKey(k0));
      assertTrue(dataContainer(p0.node(1)).containsKey(k1));
      assertTrue(dataContainer(p0.node(1)).containsKey(k2));

      assertTrue(dataContainer(p1.node(0)).containsKey(k0));
      assertTrue(dataContainer(p1.node(0)).containsKey(k1));
      assertTrue(dataContainer(p1.node(0)).containsKey(k2));

      //4. check writes on partition one
      partition(0).assertKeyAvailableForWrite(k0, "v00");
      partition(0).assertKeyAvailableForWrite(k1, "v11");
      partition(0).assertKeyAvailableForWrite(k2, "v22");

      //5. check writes on partition two
      partition(1).assertKeyNotAvailableForWrite(k0);
      partition(1).assertKeyNotAvailableForWrite(k1);
      partition(1).assertKeyNotAvailableForWrite(k2);

      partition(0).merge(partition(1));

      //use set comparison as the merge view will reshuffle the order of nodes
      assertEquals(new HashSet<>(partitionHandlingManager(0).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(1).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(2).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);

      // 4. check data seen correctly
      assertExpectedValue("v00", k0);
      assertExpectedValue("v11", k1);
      assertExpectedValue("v22", k2);

      //5. recheck key ownership
      assertTrue(dataContainer(p0.node(0)).containsKey(k0));
      assertTrue(dataContainer(p0.node(0)).containsKey(k1));
      assertTrue(dataContainer(p0.node(0)).containsKey(k2));

      assertTrue(dataContainer(p0.node(1)).containsKey(k0));
      assertTrue(dataContainer(p0.node(1)).containsKey(k1));
      assertTrue(dataContainer(p0.node(1)).containsKey(k2));

      assertTrue(dataContainer(p1.node(0)).containsKey(k0));
      assertTrue(dataContainer(p1.node(0)).containsKey(k1));
      assertTrue(dataContainer(p1.node(0)).containsKey(k2));

      cache(0).put(k0, "v000");
      cache(1).put(k1, "v111");
      cache(2).put(k2, "v222");

      assertExpectedValue("v000", k0);
      assertExpectedValue("v111", k1);
      assertExpectedValue("v222", k2);
   }
}
