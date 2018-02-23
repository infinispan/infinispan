package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.conflict.MergePolicy;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Test(groups = "functional", testName = "partitionhandling.MergePolicyRemoveAllTest")
public class MergePolicyRemoveAllTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyRemoveAllTest(REPL_SYNC, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyRemoveAllTest(REPL_SYNC, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyRemoveAllTest(REPL_SYNC, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyRemoveAllTest(REPL_SYNC, "2N", new int[]{0}, new int[]{1}),

            new MergePolicyRemoveAllTest(DIST_SYNC, "5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyRemoveAllTest(DIST_SYNC, "4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyRemoveAllTest(DIST_SYNC, "3N", new int[]{0,1}, new int[]{2}),
            new MergePolicyRemoveAllTest(DIST_SYNC, "2N", new int[]{0}, new int[]{1})
      };
   }

   public MergePolicyRemoveAllTest(){}

   public MergePolicyRemoveAllTest(CacheMode cacheMode, String description, int[] partition1, int[] partition2) {
      super(cacheMode, description, partition1, partition2);
      this.mergePolicy = MergePolicy.REMOVE_ALL;
      this.valueAfterMerge = null;
   }
}
