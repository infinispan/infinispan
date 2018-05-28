package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DeltaAwareTxPreloadTest")
public class DeltaAwareTxPreloadTest extends BaseDeltaAwarePreloadTest {

   @Override
   boolean isTx() {
      return true;
   }
}
