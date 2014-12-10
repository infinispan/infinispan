package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseNonTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.Sync2ndPhaseDistExtendedStatisticTest")
public class Sync2ndPhaseDistExtendedStatisticTest extends BaseNonTotalOrderClusteredExtendedStatisticsTest {

   public Sync2ndPhaseDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, true, false);
   }
}
