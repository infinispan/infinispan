package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncUnsafeFuncTest")
public class DistSyncUnsafeFuncTest extends DistSyncFuncTest {
   @Override
   public Object[] factory() {
      return new Object[] {
         new DistSyncUnsafeFuncTest(),
         new DistSyncUnsafeFuncTest().groupers(true)
      };
   }

   public DistSyncUnsafeFuncTest() {
      testRetVals = false;
   }
}
