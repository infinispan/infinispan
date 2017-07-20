package org.infinispan.xsite;

import org.testng.annotations.Test;

@Test (groups = "xsite", testName = "xsite.OptDistCache2PcOperationsTest")
public class OptDistCache2PcOperationsTest extends OptDistCacheOperationsTest {
   public OptDistCache2PcOperationsTest() {
      use2Pc = true;
   }
}
