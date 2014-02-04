package org.infinispan.stress;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "stress.DistWriteSkewStressTest", groups = "stress")
public class DistWriteSkewStressTest extends AbstractWriteSkewStressTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

}
