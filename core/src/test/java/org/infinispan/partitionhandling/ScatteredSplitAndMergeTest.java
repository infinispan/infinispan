package org.infinispan.partitionhandling;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.partitionhandling.AvailabilityMode.DEGRADED_MODE;
import static org.testng.Assert.*;

@Test(groups = "functional")
public class ScatteredSplitAndMergeTest extends BasePartitionHandlingTest {
   private static Log log = LogFactory.getLog(ScatteredSplitAndMergeTest.class);

   {
      cacheMode = CacheMode.SCATTERED_SYNC;
   }

   public void testSplitAndMerge1() throws Exception {
      testSplitAndMerge(
         new PartitionDescriptor(DEGRADED_MODE, 0, 1),
         new PartitionDescriptor(DEGRADED_MODE, 2),
         new PartitionDescriptor(DEGRADED_MODE, 3));
   }

   public void testSplitAndMerge2() throws Exception {
      testSplitAndMerge(
         new PartitionDescriptor(DEGRADED_MODE, 1, 2),
         new PartitionDescriptor(DEGRADED_MODE, 0),
         new PartitionDescriptor(DEGRADED_MODE, 3));
   }

   public void testSplitAndMerge3() throws Exception {
      testSplitAndMerge(
         new PartitionDescriptor(DEGRADED_MODE, 0, 1),
         new PartitionDescriptor(DEGRADED_MODE, 2, 3));
   }

   public void testSplitAndMerge4() throws Exception {
      testSplitAndMerge(
         new PartitionDescriptor(AVAILABLE, 0, 1 ,2),
         new PartitionDescriptor(DEGRADED_MODE, 3));
   }

   public void testSplitAndMerge5() throws Exception {
      testSplitAndMerge(
         new PartitionDescriptor(AVAILABLE, 1, 2, 3),
         new PartitionDescriptor(DEGRADED_MODE, 0));
   }

   private void testSplitAndMerge(PartitionDescriptor... descriptors) throws Exception {
      // Get a key for each node - the owners can't be combined
      MagicKey[] keys = IntStream.range(0, numMembersInCluster).mapToObj(
         i -> {
            MagicKey key = new MagicKey("k" + i, cache(i));
            cache(i).put(key, "v0");
            return key;
         }).toArray(MagicKey[]::new);
      String[] lastWrittenValues = new String[keys.length];
      Arrays.fill(lastWrittenValues, "v0");

      log.trace("Before split.");
      splitCluster(descriptors);
      for (int i = 0; i < descriptors.length; ++i) {
         descriptors[i].assertAvailabilityMode(partition(i));
      }


      // check writes
      for (int i = 0; i < keys.length; i++) {
         MagicKey key = keys[i];
         for (PartitionDescriptor descriptor : descriptors) {
            for (int node : descriptor.getNodes()) {
               try {
                  assertEquals(lastWrittenValues[i], cache(node).put(key, "v1"));
                  lastWrittenValues[i] = "v1";
                  assertEquals(AVAILABLE, descriptor.expectedMode, descriptor.toString());
               } catch (AvailabilityException ae) {
                  assertEquals(DEGRADED_MODE, descriptor.expectedMode);
               }
            }
         }
      }

      // check reads
      for (MagicKey key : keys) {
         for (PartitionDescriptor descriptor : descriptors) {
            for (int node : descriptor.getNodes()) {
               try {
                  // all who can read should have written
                  assertEquals(cache(node).get(key), "v1");
                  assertEquals(AVAILABLE, descriptor.expectedMode);
               } catch (AvailabilityException ae) {
                  assertEquals(DEGRADED_MODE, descriptor.expectedMode);
               }
            }
         }
      }

      for (int merge = 1; partitions.length > 1; ++merge) {
         log.tracef("Before merge #%d", merge);
         int prevPartitions = partitions.length;
         partition(0).merge(partition(1));
         assertEquals(partitions.length, prevPartitions - 1);
         log.tracef("After merge #%d", merge);

         partition(0).assertAvailabilityMode(AvailabilityMode.AVAILABLE);
         if (partitions.length > 1) {
            partition(1).assertAvailabilityMode(DEGRADED_MODE);
         }

         for (int i = 0; i < keys.length; i++) {
            MagicKey key = keys[i];
            partition(0).assertKeyAvailableForRead(key, lastWrittenValues[i]);
            String newValue = "v" + (merge + 1);
            partition(0).assertKeyAvailableForWrite(key, newValue);
            lastWrittenValues[i] = newValue;

            if (partitions.length > 1) {
               partition(1).assertKeyNotAvailableForRead(key);
               partition(1).assertKeyNotAvailableForWrite(key);
            }
         }

         partition(0).assertConsistenHashMembers(partition(0).getAddresses());
         if (partitions.length > 1) {
            partition(1).assertConsistenHashMembers(Arrays.asList(address(0), address(1), address(2), address(3)));
         }
      }
   }

}
