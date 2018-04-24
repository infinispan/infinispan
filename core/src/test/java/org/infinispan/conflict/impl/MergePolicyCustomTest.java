package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyCustomTest")
public class MergePolicyCustomTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyCustomTest(REPL_SYNC, 2, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest(REPL_SYNC, 2, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest(REPL_SYNC, 2, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyCustomTest(REPL_SYNC, 2, "2N", new int[]{0}, new int[]{1}),

            new MergePolicyCustomTest(DIST_SYNC, 1, "5N-1", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest(DIST_SYNC, 1, "4N-1", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest(DIST_SYNC, 1, "3N-1", new int[]{0,1}, new int[]{2}),
            new MergePolicyCustomTest(DIST_SYNC, 1, "2N-1", new int[]{0}, new int[]{1}),

            new MergePolicyCustomTest(DIST_SYNC, 2, "5N-2", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest(DIST_SYNC, 2, "4N-2", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest(DIST_SYNC, 2, "3N-2", new int[]{0,1}, new int[]{2}),
            new MergePolicyCustomTest(DIST_SYNC, 2, "2N-2", new int[]{0}, new int[]{1})
      };
   }

   public MergePolicyCustomTest(){}

   public MergePolicyCustomTest(CacheMode cacheMode, int owners, String description, int[] partition1, int[] partition2) {
      super(cacheMode, owners, description, AvailabilityMode.AVAILABLE, partition1, partition2);
      this.mergePolicy = ((preferredEntry, otherEntries) -> TestInternalCacheEntryFactory.create(conflictKey, "Custom"));
      this.valueAfterMerge = "Custom";
   }
}
