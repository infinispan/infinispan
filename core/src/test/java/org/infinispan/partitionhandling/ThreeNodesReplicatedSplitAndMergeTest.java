package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

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


      HashSet<Address> allMembers = new HashSet<>(advancedCache(0).getRpcManager().getMembers());
      //use set comparison as the merge view will reshuffle the order of nodes
      assertStableTopologyMembers(allMembers, partitionHandlingManager(0));
      assertStableTopologyMembers(allMembers, partitionHandlingManager(1));
      assertStableTopologyMembers(allMembers, partitionHandlingManager(2));
      for (int i = 0; i < numMembersInCluster; i++) {
         assertEquals(AvailabilityMode.AVAILABLE, partitionHandlingManager(i).getAvailabilityMode());
      }

      splitCluster(p0.getNodes(), p1.getNodes());

      TestingUtil.waitForStableTopology(cache(p0.node(0)), cache(p0.node(1)));
      partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);
      partition(1).assertAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
      assertStableTopologyMembers(allMembers, partitionHandlingManager(p1.node(0)));

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
      expectStableTopologyMembers(allMembers, partitionHandlingManager(0));
      expectStableTopologyMembers(allMembers, partitionHandlingManager(1));
      expectStableTopologyMembers(allMembers, partitionHandlingManager(2));
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

      assertNull(cache(0).get("nonExistentKey"));
   }

   private void assertStableTopologyMembers(HashSet<Address> allMembers, PartitionHandlingManager phm) {
      assertEquals(allMembers, new HashSet<Address>(phm.getLastStableTopology().getMembers()));
   }

   private void expectStableTopologyMembers(HashSet<Address> expected, PartitionHandlingManager phm) {
      eventuallyEquals(expected, () -> new HashSet<Address>(phm.getLastStableTopology().getMembers()));
   }

}
