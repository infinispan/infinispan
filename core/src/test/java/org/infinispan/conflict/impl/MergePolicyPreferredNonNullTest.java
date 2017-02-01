package org.infinispan.conflict.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.conflict.MergePolicies;
import org.infinispan.distribution.MagicKey;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyPreferredNonNullTest")
public class MergePolicyPreferredNonNullTest extends BaseMergePolicyTest {

   private MagicKey conflictKey;

   public MergePolicyPreferredNonNullTest() {
      super();
      this.mergePolicy = MergePolicies.PREFERRED_NON_NULL;
   }

   @Override
   void beforeSplit() {
      conflictKey = new MagicKey(cache(2), cache(0));
      cache(0).put(conflictKey, "BEFORE SPLIT");
   }

   @Override
   void duringSplit() {
      AdvancedCache[] caches = new AdvancedCache[] {advancedCache(0), advancedCache(2)};
      getCacheFromPreferredPartition(caches).remove(conflictKey);
      getCacheFromNonPreferredPartition(caches).put(conflictKey, "DURING SPLIT");
   }

   @Override
   void afterMerge() {
      assertSameVersionAndNoConflicts(0, 2, conflictKey, "DURING SPLIT");
   }
}
