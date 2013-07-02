package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "container.versioning.DistL1WriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class DistL1WriteSkewTest extends DistWriteSkewTest {
   @Override
   protected void decorate(ConfigurationBuilder builder) {
      // Enable L1
      builder.clustering().l1().enable();
   }
}