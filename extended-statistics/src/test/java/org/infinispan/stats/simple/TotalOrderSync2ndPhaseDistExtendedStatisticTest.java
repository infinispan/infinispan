package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSync2ndPhaseDistExtendedStatisticTest")
public class TotalOrderSync2ndPhaseDistExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected TotalOrderSync2ndPhaseDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, true, false, true);
   }
}
