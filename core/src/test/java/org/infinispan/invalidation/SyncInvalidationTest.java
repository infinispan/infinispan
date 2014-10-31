package org.infinispan.invalidation;

import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "invalidation.SyncInvalidationTest")
public class SyncInvalidationTest extends BaseInvalidationTest {
   public SyncInvalidationTest() {
      isSync = true;
   }
}
