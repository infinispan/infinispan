package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.SyncDistExtendedStatisticTest")
public class SyncDistExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected SyncDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, false, false, false);
   }
}
