package org.infinispan.conflict.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.MergePolicyCustomTest")
public class MergePolicyCustomTest extends BaseMergePolicyTest {

   MagicKey conflictKey;

   public MergePolicyCustomTest() {
      this.mergePolicy = ((preferredEntry, otherEntries) -> TestInternalCacheEntryFactory.create(conflictKey, "Custom"));
   }

   @Override
   void beforeSplit() {
      conflictKey = new MagicKey(cache(2), cache(0));
      cache(0).put(conflictKey, "BEFORE SPLIT");
   }

   @Override
   void duringSplit() {
      AdvancedCache<Object, Object> cache = getCacheFromPreferredPartition(advancedCache(0), advancedCache(2));
      cache.put(conflictKey, "DURING SPLIT");   }

   @Override
   void afterMerge() {
      assertSameVersionAndNoConflicts(0, 2, conflictKey, "Custom");
   }
}
