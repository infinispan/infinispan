package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSyncWriteSkewDistExtendedStatisticTest")
public class TotalOrderSyncWriteSkewDistExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSyncWriteSkewDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, true);
   }
}
