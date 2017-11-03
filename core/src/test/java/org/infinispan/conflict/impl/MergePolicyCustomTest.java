package org.infinispan.conflict.impl;

import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyCustomTest")
public class MergePolicyCustomTest extends BaseMergePolicyTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new MergePolicyCustomTest().setPartitions(new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyCustomTest().setPartitions(new int[]{0,1}, new int[]{2,3}),
            new MergePolicyCustomTest().setPartitions(new int[]{0,1}, new int[]{2})
      };
   }

   public MergePolicyCustomTest() {
      this.mergePolicy = ((preferredEntry, otherEntries) -> TestInternalCacheEntryFactory.create(conflictKey, "Custom"));
      this.valueAfterMerge = "Custom";
   }
}
