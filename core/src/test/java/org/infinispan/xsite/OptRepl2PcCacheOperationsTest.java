package org.infinispan.xsite;

import org.testng.annotations.Test;

@Test (groups = "xsite", testName = "xsite.OptRepl2PcCacheOperationsTest")
public class OptRepl2PcCacheOperationsTest extends OptReplCacheOperationsTest{
   public OptRepl2PcCacheOperationsTest() {
      use2Pc = true;
   }
}
