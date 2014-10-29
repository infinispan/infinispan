package org.infinispan.partitionhandling;

import org.infinispan.distribution.MagicKey;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;

import static org.testng.Assert.*;

@Test(groups = "functional", testName = "partitionhandling.TwoWaySplitAndMergeTest")
public class TwoWaySplitAndMergeTest extends BasePartitionHandlingTest {

   public void testSplitAndMerge0() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(0, 1), new PartitionDescriptor(2, 3));
   }
//
   public void testSplitAndMerge1() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(0, 2), new PartitionDescriptor(1, 3));
   }

   public void testSplitAndMerge2() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(0, 3), new PartitionDescriptor(1, 2));
   }

   public void testSplitAndMerge3() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(1, 2), new PartitionDescriptor(0, 3));
   }

   public void testSplitAndMerge4() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(1, 3), new PartitionDescriptor(0, 2));
   }

   public void testSplitAndMerge5() throws Exception {
      testSplitAndMerge(new PartitionDescriptor(2, 3), new PartitionDescriptor(0, 1));
   }

   private void testSplitAndMerge(PartitionDescriptor p0, PartitionDescriptor p1) throws Exception {
      Object k0 = new MagicKey(cache(p0.node(0)), cache(p0.node(1)));
      cache(0).put(k0, 0);

      Object k1 = new MagicKey(cache(p0.node(1)), cache(p1.node(0)));
      cache(1).put(k1, 1);

      Object k2 = new MagicKey(cache(p1.node(0)), cache(p1.node(1)));
      cache(2).put(k2, 2);

      Object k3 = new MagicKey(cache(p1.node(1)), cache(p0.node(0)));
      cache(3).put(k3, 3);


      List<Address> allMembers = advancedCache(0).getRpcManager().getMembers();
      //use set comparison as the merge view will reshuffle the order of nodes
      assertEquals(new HashSet<>(partitionHandlingManager(0).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(1).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(2).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(3).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
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
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      assertEquals(partitionHandlingManager(0).getLastStableTopology().getMembers(), allMembers);
      assertEquals(partitionHandlingManager(1).getLastStableTopology().getMembers(), allMembers);
      assertEquals(partitionHandlingManager(2).getLastStableTopology().getMembers(), allMembers);
      assertEquals(partitionHandlingManager(3).getLastStableTopology().getMembers(), allMembers);

      //1. check key visibility in partition 1
      partition(0).assertKeyAvailableForRead(k0, 0);
      partition(0).assertKeysNotAvailableForRead(k1, k2, k3);

      //2. check key visibility in partition 2
      partition(1).assertKeyAvailableForRead(k2, 2);
      partition(1).assertKeysNotAvailableForRead(k0, k1, k3);

      //3. check key ownership
      assertTrue(dataContainer(p0.node(0)).containsKey(k0));
      assertFalse(dataContainer(p0.node(0)).containsKey(k1));
      assertFalse(dataContainer(p0.node(0)).containsKey(k2));
      assertTrue(dataContainer(p0.node(0)).containsKey(k3));

      assertTrue(dataContainer(p0.node(1)).containsKey(k0));
      assertTrue(dataContainer(p0.node(1)).containsKey(k1));
      assertFalse(dataContainer(p0.node(1)).containsKey(k2));
      assertFalse(dataContainer(p0.node(1)).containsKey(k3));

      assertFalse(dataContainer(p1.node(0)).containsKey(k0));
      assertTrue(dataContainer(p1.node(0)).containsKey(k1));
      assertTrue(dataContainer(p1.node(0)).containsKey(k2));
      assertFalse(dataContainer(p1.node(0)).containsKey(k3));

      assertFalse(dataContainer(p1.node(1)).containsKey(k0));
      assertFalse(dataContainer(p1.node(1)).containsKey(k1));
      assertTrue(dataContainer(p1.node(1)).containsKey(k2));
      assertTrue(dataContainer(p1.node(1)).containsKey(k3));


      //4. check writes on partition one
      partition(0).assertKeyAvailableForWrite(k0, -1);
      partition(0).assertKeyNotAvailableForWrite(k1);
      partition(0).assertKeyNotAvailableForWrite(k2);
      partition(0).assertKeyNotAvailableForWrite(k3);

      //5. check writes on partition two
      partition(1).assertKeyAvailableForWrite(k2, -1);
      partition(1).assertKeyNotAvailableForWrite(k0);
      partition(1).assertKeyNotAvailableForWrite(k1);
      partition(1).assertKeyNotAvailableForWrite(k3);

      partition(0).merge(partition(1));

      //use set comparison as the merge view will reshuffle the order of nodes
      assertEquals(new HashSet<>(partitionHandlingManager(0).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(1).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(2).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      assertEquals(new HashSet<>(partitionHandlingManager(3).getLastStableTopology().getMembers()), new HashSet<>(allMembers));
      partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);

      // 4. check data seen correctly
      assertExpectedValue(-1, k0);
      assertExpectedValue(1, k1);
      assertExpectedValue(-1, k2);
      assertExpectedValue(3, k3);


      //5. recheck key ownership
      assertTrue(dataContainer(p0.node(0)).containsKey(k0));
      assertFalse(dataContainer(p0.node(0)).containsKey(k1));
      assertFalse(dataContainer(p0.node(0)).containsKey(k2));
      assertTrue(dataContainer(p0.node(0)).containsKey(k3));

      assertTrue(dataContainer(p0.node(1)).containsKey(k0));
      assertTrue(dataContainer(p0.node(1)).containsKey(k1));
      assertFalse(dataContainer(p0.node(1)).containsKey(k2));
      assertFalse(dataContainer(p0.node(1)).containsKey(k3));

      assertFalse(dataContainer(p1.node(0)).containsKey(k0));
      assertTrue(dataContainer(p1.node(0)).containsKey(k1));
      assertTrue(dataContainer(p1.node(0)).containsKey(k2));
      assertFalse(dataContainer(p1.node(0)).containsKey(k3));

      assertFalse(dataContainer(p1.node(1)).containsKey(k0));
      assertFalse(dataContainer(p1.node(1)).containsKey(k1));
      assertTrue(dataContainer(p1.node(1)).containsKey(k2));
      assertTrue(dataContainer(p1.node(1)).containsKey(k3));

      cache(0).put(k0, 10);
      cache(1).put(k1, 100);
      cache(2).put(k2, 1000);
      cache(3).put(k3, 10000);

      assertExpectedValue(10, k0);
      assertExpectedValue(100, k1);
      assertExpectedValue(1000, k2);
      assertExpectedValue(10000, k3);

   }
}
