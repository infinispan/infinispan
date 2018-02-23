package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyCustomTest")
public class MergePolicyCustomTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyCustomTest(REPL_SYNC, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest(REPL_SYNC, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest(REPL_SYNC, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyCustomTest(REPL_SYNC, "2N", new int[]{0}, new int[]{1}),

            new MergePolicyCustomTest(DIST_SYNC, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest(DIST_SYNC, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest(DIST_SYNC, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyCustomTest(DIST_SYNC, "2N", new int[]{0}, new int[]{1})
      };
   }

   public MergePolicyCustomTest(){}

   public MergePolicyCustomTest(CacheMode cacheMode, String description, int[] partition1, int[] partition2) {
      super(cacheMode, description, partition1, partition2);
      this.mergePolicy = ((preferredEntry, otherEntries) -> TestInternalCacheEntryFactory.create(conflictKey, "Custom"));
      this.valueAfterMerge = "Custom";
   }
}
