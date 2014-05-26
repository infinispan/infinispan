package org.infinispan.partitionhandling;

import org.infinispan.distribution.MagicKey;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

@Test(groups = "functional", testName = "partitionhandling.TwoWaySplitAndMergeTest")
public class TwoWaySplitAndMergeTest extends BasePartitionHandlingTest {

   public void testSplitAndMerge() throws Exception {
      Object k0 = new MagicKey(cache(0), cache(1));
      cache(0).put(k0, 0);

      Object k1 = new MagicKey(cache(1), cache(2));
      cache(1).put(k1, 1);

      Object k2 = new MagicKey(cache(2), cache(3));
      cache(2).put(k2, 2);

      Object k3 = new MagicKey(cache(3), cache(0));
      cache(3).put(k3, 3);


      List<Address> allMembers = advancedCache(0).getRpcManager().getMembers();
      assertEquals(partitionHandlingManager(0).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(1).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(2).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(3).getLastStableCluster(), allMembers);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < 4; i++)
               if (partitionHandlingManager(0).getState() != PartitionHandlingManager.PartitionState.AVAILABLE)
                  return false;
            return true;
         }
      });

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      assertEquals(partitionHandlingManager(0).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(1).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(2).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(3).getLastStableCluster(), allMembers);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < 4; i++)
               if (partitionHandlingManager(0).getState() != PartitionHandlingManager.PartitionState.DEGRADED_MODE)
                  return false;
            return true;
         }
      });

      //1. check key visibility in partition 1
      partition(0).assertKeyAvailableForRead(k0, 0);
      partition(0).assertKeysNotAvailable(k1, k2, k3);

      //2. check key visibility in partition 2
      partition(1).assertKeyAvailableForRead(k2, 2);
      partition(1).assertKeysNotAvailable(k0, k1, k3);

      //3. check key ownership
      assertTrue(dataContainer(0).containsKey(k0));
      assertFalse(dataContainer(0).containsKey(k1));
      assertFalse(dataContainer(0).containsKey(k2));
      assertTrue(dataContainer(0).containsKey(k3));

      assertTrue(dataContainer(1).containsKey(k0));
      assertTrue(dataContainer(1).containsKey(k1));
      assertFalse(dataContainer(1).containsKey(k2));
      assertFalse(dataContainer(1).containsKey(k3));

      assertFalse(dataContainer(2).containsKey(k0));
      assertTrue(dataContainer(2).containsKey(k1));
      assertTrue(dataContainer(2).containsKey(k2));
      assertFalse(dataContainer(2).containsKey(k3));

      assertFalse(dataContainer(3).containsKey(k0));
      assertFalse(dataContainer(3).containsKey(k1));
      assertTrue(dataContainer(3).containsKey(k2));
      assertTrue(dataContainer(3).containsKey(k3));


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

      assertEquals(partitionHandlingManager(0).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(1).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(2).getLastStableCluster(), allMembers);
      assertEquals(partitionHandlingManager(3).getLastStableCluster(), allMembers);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < 4; i++)
               if (partitionHandlingManager(0).getState() != PartitionHandlingManager.PartitionState.AVAILABLE)
                  return false;
            return true;
         }
      });

      // 4. check data seen correctly
      assertExpectedValue(-1, k0);
      assertExpectedValue(1, k1);
      assertExpectedValue(-1, k2);
      assertExpectedValue(3, k3);


      //5. recheck key ownership
      assertTrue(dataContainer(0).containsKey(k0));
      assertFalse(dataContainer(0).containsKey(k1));
      assertFalse(dataContainer(0).containsKey(k2));
      assertTrue(dataContainer(0).containsKey(k3));

      assertTrue(dataContainer(1).containsKey(k0));
      assertTrue(dataContainer(1).containsKey(k1));
      assertFalse(dataContainer(1).containsKey(k2));
      assertFalse(dataContainer(1).containsKey(k3));

      assertFalse(dataContainer(2).containsKey(k0));
      assertTrue(dataContainer(2).containsKey(k1));
      assertTrue(dataContainer(2).containsKey(k2));
      assertFalse(dataContainer(2).containsKey(k3));

      assertFalse(dataContainer(3).containsKey(k0));
      assertFalse(dataContainer(3).containsKey(k1));
      assertTrue(dataContainer(3).containsKey(k2));
      assertTrue(dataContainer(3).containsKey(k3));

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
