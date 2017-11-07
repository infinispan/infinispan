package org.infinispan.conflict.impl;

import org.infinispan.conflict.MergePolicies;
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
            new MergePolicyRemoveAllTest().setPartitions("5N", new int[]{0,1,2}, new int[]{3,4}),
            new MergePolicyRemoveAllTest().setPartitions("4N", new int[]{0,1}, new int[]{2,3}),
            new MergePolicyRemoveAllTest().setPartitions("3N", new int[]{0,1}, new int[]{2})
      };
   }

   public MergePolicyRemoveAllTest() {
      super();
      this.mergePolicy = MergePolicies.REMOVE_ALL;
      this.valueAfterMerge = null;
   }
}
