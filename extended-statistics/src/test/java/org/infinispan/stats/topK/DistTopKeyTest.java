package org.infinispan.stats.topK;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.DistTopKeyTest")
public class DistTopKeyTest extends BaseClusterTopKeyTest {
   public DistTopKeyTest() {
      super(CacheMode.DIST_SYNC, 2);
   }
}
