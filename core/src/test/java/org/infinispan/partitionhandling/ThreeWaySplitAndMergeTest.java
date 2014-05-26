package org.infinispan.partitionhandling;

import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.*;

@Test(groups = "functional", testName = "partitionhandling.ThreeWaySplitAndMergeTest")
public class ThreeWaySplitAndMergeTest extends BasePartitionHandlingTest {

   private static Log log = LogFactory.getLog(ThreeWaySplitAndMergeTest.class);

   public void testSplitAndMerge() throws Exception {
      Object k0 = new MagicKey(cache(0), cache(1));
      cache(0).put(k0, 0);

      Object k1 = new MagicKey(cache(1), cache(2));
      cache(1).put(k1, 1);

      Object k2 = new MagicKey(cache(2), cache(3));
      cache(2).put(k2, 2);

      Object k3 = new MagicKey(cache(3), cache(0));
      cache(3).put(k3, 3);

      log.trace("Before first split.");
      splitCluster(new int[]{0, 1}, new int[]{2}, new int[]{3});

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (int i = 0; i < 4; i++)
               if (partitionHandlingManager(0).getState() != PartitionHandlingManager.PartitionState.DEGRADED_MODE)
                  return false;
            return true;
         }
      });

      //1. check key visibility in partition 0
      partition(0).assertKeyAvailableForRead(k0, 0);
      partition(0).assertKeysNotAvailable(k1, k2, k3);

      //2. check key visibility in partition 1
      partition(1).assertKeysNotAvailable(k0, k1, k2, k3);

      //3. check key visibility in partition 2
      partition(2).assertKeysNotAvailable(k0, k1, k2, k3);

      //4. check key ownership
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


      //5. check writes on partition 0
      partition(0).assertKeyAvailableForWrite(k0, -1);

      //5. check writes on partition 1
      partition(1).assertKeysNotAvailableForWrite(k0, k1, k2);

      //6. check writes on partition 2
      partition(2).assertKeysNotAvailableForWrite(k0, k1, k2);


      log.tracef("before_merge");
      assertEquals(partitions.length, 3);
      partition(0).merge(partition(1));
      assertEquals(partitions.length, 2);

      partition(0).expectPartitionState(PartitionHandlingManager.PartitionState.AVAILABLE);
      partition(1).expectPartitionState(PartitionHandlingManager.PartitionState.DEGRADED_MODE);

      partition(0).assertKeyAvailableForRead(k0, -1);
      partition(0).assertKeyAvailableForRead(k1, 1);
      partition(0).assertKeyAvailableForRead(k2, 2);
      partition(0).assertKeyAvailableForRead(k3, 3);

      partition(0).assertKeyAvailableForWrite(k0, 10);
      partition(0).assertKeyAvailableForWrite(k1, 11);
      partition(0).assertKeyAvailableForWrite(k2, 12);
      partition(0).assertKeyAvailableForWrite(k3, 13);

      Set<Address> members = new HashSet<Address>(Arrays.asList(new Address[]{address(0), address(1), address(2)}));
      assertEquals(new HashSet<Address>(advancedCache(0).getDistributionManager().getConsistentHash().getMembers()), members);
      assertEquals(new HashSet<Address>(advancedCache(1).getDistributionManager().getConsistentHash().getMembers()), members);
      assertEquals(new HashSet<Address>(advancedCache(2).getDistributionManager().getConsistentHash().getMembers()), members);

      partition(1).assertKeysNotAvailable(k0, k1, k2, k3);

      members = new HashSet<Address>(Arrays.asList(new Address[]{address(0), address(1), address(2), address(3)}));
      assertEquals(new HashSet<Address>(advancedCache(3).getDistributionManager().getConsistentHash().getMembers()), members);

      for (int i = 0; i < 10000; i++)
         dataContainer(3).put(i, i, null);

      log.trace("Before the 2nd partition.");
      log.tracef("P0=%s, P1 = %s", partition(0), partition(1));
      partition(0).merge(partition(1));
      log.tracef("After P0=%s", partition(0));


      assertEquals(partitions.length, 1);
      partition(0).expectPartitionState(PartitionHandlingManager.PartitionState.AVAILABLE);

      partition(0).assertKeyAvailableForRead(k0, 10);
      partition(0).assertKeyAvailableForRead(k1, 11);
      partition(0).assertKeyAvailableForRead(k2, 12);
      partition(0).assertKeyAvailableForRead(k3, 13);

      for (int i = 0; i < 10000; i++) {
         assertNull(cache(3).get(i));
         assertNull(dataContainer(3).get(i));
      }


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
