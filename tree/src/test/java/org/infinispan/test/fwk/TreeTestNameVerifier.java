package org.infinispan.test.fwk;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "test.fwk.TreeTestNameVerifier")
public class TreeTestNameVerifier extends TestNameVerifier {
   public TreeTestNameVerifier() {
      moduleName = "tree";
   }
}
