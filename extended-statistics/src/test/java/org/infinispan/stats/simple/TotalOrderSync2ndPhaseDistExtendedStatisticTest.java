package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSync2ndPhaseDistExtendedStatisticTest")
public class TotalOrderSync2ndPhaseDistExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSync2ndPhaseDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, true, false);
   }
}
