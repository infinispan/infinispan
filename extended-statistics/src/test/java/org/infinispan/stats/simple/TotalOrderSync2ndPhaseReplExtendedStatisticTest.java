package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSync2ndPhaseReplExtendedStatisticTest")
public class TotalOrderSync2ndPhaseReplExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSync2ndPhaseReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, true, false);
   }
}
