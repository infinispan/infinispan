package org.infinispan.test.fwk;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "test.fwk.BdbjeTestNameVerifier")
public class BdbjeTestNameVerifier extends TestNameVerifier {
   public BdbjeTestNameVerifier() {
      moduleName = "cachestore/bdbje";
   }
}
