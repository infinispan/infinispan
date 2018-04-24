package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyPreferredAlwaysTest")
public class MergePolicyPreferredAlwaysTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyPreferredAlwaysTest(REPL_SYNC, 2, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyPreferredAlwaysTest(REPL_SYNC, 2, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyPreferredAlwaysTest(REPL_SYNC, 2, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyPreferredAlwaysTest(REPL_SYNC, 2, "2N", new int[]{0}, new int[]{1}),

            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 1, "5N-1", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 1, "4N-1", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 1, "3N-1", new int[]{0,1}, new int[]{2}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 1, "2N-1", new int[]{0}, new int[]{1}),

            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 2, "5N-2", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 2, "4N-2", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 2, "3N-2", new int[]{0,1}, new int[]{2}),
            new MergePolicyPreferredAlwaysTest(DIST_SYNC, 2, "2N-2", new int[]{0}, new int[]{1})
      };
   }

   public MergePolicyPreferredAlwaysTest(){}

   public MergePolicyPreferredAlwaysTest(CacheMode cacheMode, int owners, String description, int[] partition1, int[] partition2) {
      super(cacheMode, owners, description, AvailabilityMode.AVAILABLE, partition1, partition2);
      this.mergePolicy = MergePolicy.PREFERRED_ALWAYS;
   }
}
