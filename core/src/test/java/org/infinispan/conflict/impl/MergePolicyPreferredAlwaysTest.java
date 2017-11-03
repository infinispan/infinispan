package org.infinispan.conflict.impl;

import org.infinispan.conflict.MergePolicies;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyPreferredAlwaysTest")
public class MergePolicyPreferredAlwaysTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyPreferredAlwaysTest().setPartitions(new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyPreferredAlwaysTest().setPartitions(new int[]{0,1}, new int[]{2,3}),
            new MergePolicyPreferredAlwaysTest().setPartitions(new int[]{0,1}, new int[]{2}).setValueAfterMerge("BEFORE SPLIT")
      };
   }

   public MergePolicyPreferredAlwaysTest() {
      super();
      this.mergePolicy = MergePolicies.PREFERRED_ALWAYS;
   }
}
