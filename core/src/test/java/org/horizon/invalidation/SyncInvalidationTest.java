package org.horizon.invalidation;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "invalidation.SyncInvalidationTest")
public class SyncInvalidationTest extends BaseInvalidationTest {
   public SyncInvalidationTest() {
      isSync = true;
   }
}
