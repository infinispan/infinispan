package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "container.versioning.DistWriteSkewConsistencyTest")
@CleanupAfterMethod
public class DistWriteSkewConsistencyTest extends ReplWriteSkewConsistencyTest {

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.clustering().hash().numOwners(2);
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
