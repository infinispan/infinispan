package org.infinispan.distribution;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DeltaAwareNonTxPreloadTest")
public class DeltaAwareNonTxPreloadTest extends BaseDeltaAwarePreloadTest {

   @Override
   boolean isTx() {
      return false;
   }
}
