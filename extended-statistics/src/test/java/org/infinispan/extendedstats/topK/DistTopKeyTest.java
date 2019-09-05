package org.infinispan.extendedstats.topK;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.topK.DistTopKeyTest")
public class DistTopKeyTest extends BaseClusterTopKeyTest {
   public DistTopKeyTest() {
      super(CacheMode.DIST_SYNC, 2);
   }
}
