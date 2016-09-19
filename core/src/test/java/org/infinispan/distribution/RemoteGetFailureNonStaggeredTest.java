package org.infinispan.distribution;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional")
@CleanupAfterMethod
public class RemoteGetFailureNonStaggeredTest extends RemoteGetFailureTest {
   public RemoteGetFailureNonStaggeredTest() {
      staggered = false;
   }
}
