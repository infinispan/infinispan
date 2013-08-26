package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(testName = "container.versioning.DistL1WriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class DistL1WriteSkewTest extends DistWriteSkewTest {
   @Override
   protected void decorate(ConfigurationBuilder builder) {
      // Enable L1
      builder.clustering().l1().enable();
   }

   public void testL1Enabled() {
      for (Cache cache : caches()) {
         AssertJUnit.assertTrue("L1 not enabled for " + address(cache),
                                cache.getCacheConfiguration().clustering().l1().enabled());
      }
   }
}
