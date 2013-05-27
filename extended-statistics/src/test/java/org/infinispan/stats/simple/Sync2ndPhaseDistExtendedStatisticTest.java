package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.Sync2ndPhaseDistExtendedStatisticTest")
public class Sync2ndPhaseDistExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected Sync2ndPhaseDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, true, false, false);
   }
}
