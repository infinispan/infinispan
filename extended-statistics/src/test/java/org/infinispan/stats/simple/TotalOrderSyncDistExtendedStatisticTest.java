package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSyncDistExtendedStatisticTest")
public class TotalOrderSyncDistExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSyncDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, false, false);
   }
}
