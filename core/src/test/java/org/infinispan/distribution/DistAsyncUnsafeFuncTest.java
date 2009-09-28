package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistAsyncUnsafeFuncTest", enabled = false)
public class DistAsyncUnsafeFuncTest extends DistAsyncFuncTest {
   public DistAsyncUnsafeFuncTest() {
      sync = false;
      tx = false;
      testRetVals = false;
   }
}
